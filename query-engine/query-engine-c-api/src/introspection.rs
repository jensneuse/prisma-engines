use introspection_connector::{ConnectorResult, DatabaseMetadata, IntrospectionConnector, IntrospectionResultOutput, ConnectorError, IntrospectionResult};
use serde_derive::*;
use sql_introspection_connector::SqlIntrospectionConnector;
use introspection_core::Error;
use datamodel::{Configuration, Datamodel};
use std::ptr::null;
use datamodel::diagnostics::{Validated, ValidatedConfiguration};

pub enum Result<T, E> {
    Ok(T),
    Err(E),
}

pub struct Introspection {}

impl Introspection {
    pub async fn introspect(schema: String) -> Result<String, Error> {
        let config = match datamodel::parse_configuration(&schema)
            .map_err(|diagnostics| Error::DatamodelError(diagnostics.to_pretty_string("schema.prisma", &schema))) {
            Ok(config) => config,
            Err(e) => {
                return Result::Err(Error::DatamodelError(e.to_string()))
            }
        };

        let ds = config.subject
            .datasources
            .first().unwrap();

        let url = ds.load_url().unwrap();

        let connector = match SqlIntrospectionConnector::new(url.as_str()).await {
            introspection_connector::ConnectorResult::Ok(connector) => connector,
            introspection_connector::ConnectorResult::Err(e) => return Result::Err(Error::ConnectorError(e)),
        };
        let datamodel = Datamodel::new();
        let result = match connector.introspect(&datamodel).await {
            Ok(introspection_result) => {
                if introspection_result.data_model.is_empty() {
                    Result::Err(Error::IntrospectionResultEmpty(url.to_string()))
                } else {
                    Result::Ok(datamodel::render_datamodel_and_config_to_string(&Datamodel {
                        models: introspection_result.data_model.models,
                        enums: introspection_result.data_model.enums,
                    }, &config.subject))
                }
            }
            Err(e) => Result::Err(Error::ConnectorError(e)),
        };
        result
    }
}