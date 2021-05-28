use crate::context::PrismaContext;
use datamodel::{Configuration, Datamodel};
use introspection_core::Error;
use introspection_connector::{ConnectorResult, DatabaseMetadata, IntrospectionConnector, IntrospectionResultOutput, ConnectorError, IntrospectionResult};
use serde_derive::*;
use sql_introspection_connector::SqlIntrospectionConnector;
use std::ptr::null;
use datamodel::diagnostics::{Validated, ValidatedConfiguration};
use crate::error::PrismaError;
use std::ops::Deref;
use request_handlers::{GraphQlHandler, GraphQlBody, SingleQuery, GraphQLProtocolAdapter, PrismaResponse};
use request_handlers::GraphQlBody::Single;
use std::sync::Arc;
use tracing_futures::Instrument;
use tracing::Level;
use graphql_parser;
use query_core::{CoreError, ResponseData};

#[derive(Debug,Clone)]
pub struct Engine {
    pub ctx: Arc<PrismaContext>,
}

struct EngineIntrospectionResult {
    datamodel: Datamodel,
    configuration: Configuration,
}

impl Engine {
    pub async fn new(schema: String) -> Result<Self, Error> {
        let options = match Engine::introspect(schema).await {
            Ok(options) => options,
            Err(e) => return Err(e)
        };
        let ctx = match PrismaContext::builder(options.configuration, options.datamodel).build().await {
            Ok(ctx) => ctx,
            Err(e) => return Err(Error::Generic(String::from("prisma error")))
        };
        let engine = Engine {
            ctx: Arc::new(ctx),
        };

        for n in 1..20 {
            let result = engine.execute().await;
            println!("result(Engine::new:{}): {}",n, result);
        }

        Ok(engine)
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
    pub async fn execute(&self) -> String {
        let data = r#"
        {
            "query": "query AllUsers {findManyusers(take: 20){id name email}}",
            "variables": {}
        }"#;

        let body: GraphQlBody = serde_json::from_str(data).unwrap();
        let ctx = self.ctx.clone();
        let handler = GraphQlHandler::new(ctx.executor(), ctx.query_schema());
        let result = handler.handle(body).await;
        return serde_json::to_string(&result).unwrap();
    }
    pub async fn _execute(&self) -> String {
        let data = r#"
        {
            "query": "query AllUsers {findManyusers(take: 20){id name email}}",
            "variables": {}
        }"#;

        let body: GqlQuery = serde_json::from_str(data).unwrap();
        let gql_doc = graphql_parser::parse_query(&body.query);
        let doc = match gql_doc {
            Ok(doc) => doc,
            Err(_) => return String::from("invalid query")
        };
        let operation = GraphQLProtocolAdapter::convert(doc, None).unwrap();
        let ctx = self.ctx.clone();
        let response = match ctx.executor().execute(operation,ctx.query_schema().clone()).await {
            Ok(res) => res,
            Err(e) => {
                return String::from("core error")
            }
        };
        let prisma_response = PrismaResponse::Single(response.into());
        return serde_json::to_string(&prisma_response).unwrap();
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GqlQuery {
    query: String
}