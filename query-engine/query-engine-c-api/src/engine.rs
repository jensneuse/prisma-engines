use crate::{error::ApiError};
use datamodel::{diagnostics::ValidatedConfiguration, Datamodel, Configuration};
use opentelemetry::global;
use prisma_models::DatamodelConverter;
use query_core::{exec_loader, schema_builder, BuildMode, QueryExecutor, QuerySchema, QuerySchemaRenderer};
use request_handlers::{
    dmmf::{self, DataModelMetaFormat},
    GraphQLSchemaRenderer, GraphQlBody, GraphQlHandler, PrismaResponse,
};
use serde::{Deserialize, Deserializer, Serialize};
use std::{
    collections::{BTreeMap, HashMap},
    path::PathBuf,
    str::FromStr,
    sync::Arc,
};
use tokio::sync::RwLock;
use tracing::{metadata::LevelFilter, Level};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use introspection_core::Error;
use sql_introspection_connector::SqlIntrospectionConnector;
use introspection_connector::IntrospectionConnector;
use datamodel::diagnostics::{Validated, Diagnostics};

/// The main engine, that can be cloned between threads when using JavaScript
/// promises.
#[derive(Clone)]
pub struct QueryEngine {
    inner: Arc<RwLock<Inner>>,
}

/// The state of the engine.
pub enum Inner {
    /// Not connected, holding all data to form a connection.
    Builder(EngineBuilder),
    /// A connected engine, holding all data to disconnect and form a new
    /// connection. Allows querying when on this state.
    Connected(ConnectedEngine),
}

/// Holding the information to reconnect the engine if needed.
#[derive(Debug, Clone)]
struct EngineDatamodel {
    datasource_overrides: Vec<(String, String)>,
    ast: Datamodel,
    raw: String,
}

/// Everything needed to connect to the database and have the core running.
pub struct EngineBuilder {
    datamodel: EngineDatamodel,
    config: ValidatedConfiguration,
}

//pub(crate) type Result<T> = std::result::Result<T, ApiError>;
pub(crate) type Executor = Box<dyn QueryExecutor + Send + Sync>;

/// Internal structure for querying and reconnecting with the engine.
pub struct ConnectedEngine {
    datamodel: EngineDatamodel,
    config: serde_json::Value,
    query_schema: Arc<QuerySchema>,
    executor: Executor,
}

/// Returned from the `serverInfo` method in javascript.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ServerInfo {
    commit: String,
    version: String,
    primary_connector: Option<String>,
}

impl ConnectedEngine {
    /// The schema AST for Query Engine core.
    pub fn query_schema(&self) -> &Arc<QuerySchema> {
        &self.query_schema
    }

    /// The query executor.
    pub fn executor(&self) -> &(dyn QueryExecutor + Send + Sync) {
        &*self.executor
    }
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct TelemetryOptions {
    enabled: bool,
    endpoint: Option<String>,
}

struct EngineIntrospectionResult {
    datamodel: Datamodel,
    configuration: Configuration,
}

impl QueryEngine {
    /// Parse a validated datamodel and configuration to allow connecting later on.
    pub fn new(schema: String) -> Result<Self, Error> {
        set_panic_hook();

        let options = match crate::RUNTIME.block_on(QueryEngine::introspect(schema.clone())) {
            Ok(options) => options,
            Err(e) => return Err(e)
        };

        let raw = datamodel::render_datamodel_and_config_to_string(&options.datamodel, &options.configuration);

        let datamodel = EngineDatamodel {
            ast: options.datamodel,
            raw: schema.clone(),
            datasource_overrides: Vec::new(),
        };

        let config = match datamodel::parse_configuration(raw.as_str()) {
            Ok(c) => c,
            Err(e) => return Err(Error::DatamodelError(e.to_string()))
        };

        let builder = EngineBuilder {
            config,
            datamodel,
        };

        Ok(Self {
            inner: Arc::new(RwLock::new(Inner::Builder(builder))),
        })
    }

    async fn introspect(schema: String) -> Result<EngineIntrospectionResult, Error> {
        let config = match datamodel::parse_configuration(&schema)
            .map_err(|diagnostics| Error::DatamodelError(diagnostics.to_pretty_string("schema.prisma", &schema))) {
            Ok(config) => config,
            Err(e) => {
                return Result::Err(Error::DatamodelError(e.to_string()));
            }
        };

        let ds = config.subject
            .datasources
            .first().unwrap();

        let url = ds.load_url().unwrap();

        let connector = match SqlIntrospectionConnector::new(url.as_str()).await {
            introspection_connector::ConnectorResult::Ok(connector) => connector,
            introspection_connector::ConnectorResult::Err(_) => panic!("unable to connect"),
        };
        let datamodel = Datamodel::new();
        let result = match connector.introspect(&datamodel).await {
            Ok(introspection_result) => {
                if introspection_result.data_model.is_empty() {
                    Result::Err(Error::IntrospectionResultEmpty(url.to_string()))
                } else {
                    Result::Ok(EngineIntrospectionResult {
                        datamodel: introspection_result.data_model,
                        configuration: config.subject,
                    })
                }
            }
            Err(e) => Result::Err(Error::ConnectorError(e)),
        };
        result
    }

    /// Connect to the database, allow queries to be run.
    pub async fn connect(&self) -> Result<(), ApiError> {
        let mut inner = self.inner.write().await;

        match *inner {
            Inner::Builder(ref builder) => {
                let template = DatamodelConverter::convert(&builder.datamodel.ast);

                // We only support one data source & generator at the moment, so take the first one (default not exposed yet).
                let data_source = builder
                    .config
                    .subject
                    .datasources
                    .first()
                    .ok_or_else(|| ApiError::configuration("No valid data source found"))?;

                let preview_features: Vec<_> = builder.config.subject.preview_features().cloned().collect();
                let url = data_source
                    .load_url()
                    .map_err(|err| crate::error::ApiError::Conversion(err, builder.datamodel.raw.clone()))?;

                let (db_name, executor) = exec_loader::load(&data_source, &preview_features, &url).await?;
                let connector = executor.primary_connector();
                connector.get_connection().await?;

                // Build internal data model
                let internal_data_model = template.build(db_name);

                let query_schema = schema_builder::build(
                    internal_data_model,
                    BuildMode::Modern,
                    false,
                    data_source.capabilities(),
                    preview_features,
                );

                let config = datamodel::json::mcf::config_to_mcf_json_value(&builder.config);

                let engine = ConnectedEngine {
                    datamodel: builder.datamodel.clone(),
                    query_schema: Arc::new(query_schema),
                    executor,
                    config,
                };

                *inner = Inner::Connected(engine);

                Ok(())
            }
            Inner::Connected(_) => Err(ApiError::AlreadyConnected),
        }
    }

    /// Disconnect and drop the core. Can be reconnected later with `#connect`.
    pub async fn disconnect(&self) -> Result<(), ApiError> {
        let mut inner = self.inner.write().await;

        match *inner {
            Inner::Connected(ref engine) => {
                let config = datamodel::parse_configuration_with_url_overrides(
                    &engine.datamodel.raw,
                    engine.datamodel.datasource_overrides.clone(),
                )
                    .map_err(|errors| ApiError::conversion(errors, &engine.datamodel.raw))?;

                let builder = EngineBuilder {
                    datamodel: engine.datamodel.clone(),
                    config,
                };

                *inner = Inner::Builder(builder);

                Ok(())
            }
            Inner::Builder(_) => Err(ApiError::NotConnected),
        }
    }

    /// If connected, sends a query to the core and returns the response.
    pub async fn query(&self, query: GraphQlBody) -> Result<PrismaResponse, ApiError> {
        match *self.inner.read().await {
            Inner::Connected(ref engine) => {
                let handler = GraphQlHandler::new(engine.executor(), engine.query_schema());
                Ok(handler.handle(query).await)
            }
            Inner::Builder(_) => Err(ApiError::NotConnected),
        }
    }

    /// Loads the query schema. Only available when connected.
    pub async fn sdl_schema(&self) -> Result<String, ApiError> {
        match *self.inner.read().await {
            Inner::Connected(ref engine) => Ok(GraphQLSchemaRenderer::render(engine.query_schema().clone())),
            Inner::Builder(_) => Err(ApiError::NotConnected),
        }
    }

    /// Loads the DMMF. Only available when connected.
    pub async fn dmmf(&self) -> Result<DataModelMetaFormat, ApiError> {
        match *self.inner.read().await {
            Inner::Connected(ref engine) => {
                let dmmf = dmmf::render_dmmf(&engine.datamodel.ast, engine.query_schema().clone());

                Ok(dmmf)
            }
            Inner::Builder(_) => Err(ApiError::NotConnected),
        }
    }

    /// Loads the configuration.
    pub async fn get_config(&self) -> Result<serde_json::Value, ApiError> {
        match *self.inner.read().await {
            Inner::Connected(ref engine) => Ok(engine.config.clone()),
            Inner::Builder(ref builder) => {
                let value = datamodel::json::mcf::config_to_mcf_json_value(&builder.config);
                Ok(value)
            }
        }
    }

    /// Info about the runnings server.
    pub async fn server_info(&self) -> Result<ServerInfo, ApiError> {
        match *self.inner.read().await {
            Inner::Connected(ref engine) => Ok(ServerInfo {
                //commit: env!("GIT_HASH").into(),
                commit: String::from("unknown"),
                //version: env!("CARGO_PKG_VERSION").into(),
                version: String::from("unknown"),
                primary_connector: Some(engine.executor().primary_connector().name()),
            }),
            Inner::Builder(_) => Ok(ServerInfo {
                //commit: env!("GIT_HASH").into(),
                commit: String::from("unknown"),
                version: String::from("unknown"),
                primary_connector: None,
            }),
        }
    }
}

pub fn set_panic_hook() {
    let original_hook = std::panic::take_hook();

    std::panic::set_hook(Box::new(move |info| {
        let payload = info
            .payload()
            .downcast_ref::<String>()
            .map(Clone::clone)
            .unwrap_or_else(|| info.payload().downcast_ref::<&str>().unwrap().to_string());

        match info.location() {
            Some(location) => {
                tracing::event!(
                    tracing::Level::ERROR,
                    message = "PANIC",
                    reason = payload.as_str(),
                    file = location.file(),
                    line = location.line(),
                    column = location.column(),
                );
            }
            None => {
                tracing::event!(tracing::Level::ERROR, message = "PANIC", reason = payload.as_str());
            }
        }

        original_hook(info)
    }));
}
