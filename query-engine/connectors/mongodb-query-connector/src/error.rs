use connector_interface::error::{ConnectorError, ErrorKind, MultiError};
use itertools::Itertools;
use mongodb::{bson::oid, error::Error as DriverError};
use regex::Regex;
use thiserror::Error;
use user_facing_errors::query_engine::DatabaseConstraint;

#[derive(Debug, Error)]
pub enum MongoError {
    #[error("Unsupported MongoDB feature: {0}.")]
    Unsupported(String),

    /// Generic conversion error.
    #[error("Failed to convert '{}' to '{}'.", from, to)]
    ConversionError { from: String, to: String },

    /// ObjectID specific conversion error.
    #[error("Malformed ObjectID: {0}.")]
    MalformedObjectId(String),

    /// Raw Mongo driver error
    #[error("{0}")]
    DriverError(#[from] DriverError),
}

// Error translation is WIP.
impl MongoError {
    pub fn into_connector_error(self) -> ConnectorError {
        match self {
            MongoError::Unsupported(feature) => ConnectorError::from_kind(ErrorKind::UnsupportedFeature(feature)),

            err @ MongoError::ConversionError { .. } => {
                ConnectorError::from_kind(ErrorKind::ConversionError(err.into()))
            }

            err @ MongoError::MalformedObjectId(_) => ConnectorError::from_kind(ErrorKind::ConversionError(err.into())),

            MongoError::DriverError(err) => match err.kind.as_ref() {
                mongodb::error::ErrorKind::AddrParse(_) => {
                    ConnectorError::from_kind(ErrorKind::ConnectionError(err.clone().into()))
                }
                mongodb::error::ErrorKind::ArgumentError { .. } => {
                    ConnectorError::from_kind(ErrorKind::QueryError(Box::new(err.clone())))
                }
                mongodb::error::ErrorKind::AuthenticationError { message, .. } => {
                    // Todo this mapping is only half correct.
                    ConnectorError::from_kind(ErrorKind::AuthenticationFailed { user: message.clone() })
                }

                mongodb::error::ErrorKind::WriteError(write_failure) => match write_failure {
                    mongodb::error::WriteFailure::WriteConcernError(concern_error) => match concern_error.code {
                        11000 => ConnectorError::from_kind(unique_violation_error(concern_error.message.as_str())),
                        code => ConnectorError::from_kind(ErrorKind::RawError {
                            code: code.to_string(),
                            message: concern_error.message.clone(),
                        }),
                    },

                    mongodb::error::WriteFailure::WriteError(write_error) => match write_error.code {
                        11000 => ConnectorError::from_kind(unique_violation_error(write_error.message.as_str())),
                        code => ConnectorError::from_kind(ErrorKind::RawError {
                            code: code.to_string(),
                            message: write_error.message.clone(),
                        }),
                    },

                    _ => ConnectorError::from_kind(ErrorKind::QueryError(Box::new(err.clone()))),
                },

                mongodb::error::ErrorKind::BulkWriteError(err) => {
                    let mut errors = match err.write_errors {
                        Some(ref errors) => errors
                            .iter()
                            .map(|err| match err.code {
                                11000 => unique_violation_error(err.message.as_str()),
                                _ => ErrorKind::RawError {
                                    code: err.code.to_string(),
                                    message: format!(
                                        "Bulk write error on write index '{}': {}",
                                        err.index, err.message
                                    ),
                                },
                            })
                            .collect_vec(),

                        None => vec![],
                    };

                    if let Some(ref err) = err.write_concern_error {
                        let kind = match err.code {
                            11000 => unique_violation_error(err.message.as_str()),
                            _ => ErrorKind::RawError {
                                code: err.code.to_string(),
                                message: format!("Bulk write concern error: {}", err.message),
                            },
                        };

                        errors.push(kind);
                    };

                    ConnectorError::from_kind(ErrorKind::MultiError(MultiError { errors }))
                }

                mongodb::error::ErrorKind::BsonDecode(err) => ConnectorError::from_kind(
                    ErrorKind::InternalConversionError(format!("BSON decode error: {}", err)),
                ),

                mongodb::error::ErrorKind::BsonEncode(err) => ConnectorError::from_kind(
                    ErrorKind::InternalConversionError(format!("BSON encode error: {}", err)),
                ),

                mongodb::error::ErrorKind::DnsResolve(_) => {
                    ConnectorError::from_kind(ErrorKind::ConnectionError(err.clone().into()))
                }

                _ => ConnectorError::from_kind(ErrorKind::RawError {
                    code: "unknown".to_owned(),
                    message: format!("{}", err),
                })

                // Currently explicitly unhandled errors. If the need arises to surface them
                // in a special way, add them to the handled errors above.
                // mongodb::error::ErrorKind::InvalidDnsName(_) => todo!(),
                // mongodb::error::ErrorKind::NoDnsResults(_) => todo!(),
                // mongodb::error::ErrorKind::SrvLookupError { .. } => todo!(),
                // mongodb::error::ErrorKind::ServerSelectionError { .. } => todo!(),
                // mongodb::error::ErrorKind::InvalidHostname { .. } => todo!(),
                // mongodb::error::ErrorKind::ConnectionPoolClearedError { .. } => todo!(),
                // mongodb::error::ErrorKind::CommandError(_) => todo!(),
                // mongodb::error::ErrorKind::InternalError { .. } => todo!(),
                // mongodb::error::ErrorKind::Io(_) => todo!(),
                // mongodb::error::ErrorKind::OperationError { .. } => todo!(),
                // mongodb::error::ErrorKind::ParseError { .. } => todo!(),
                // mongodb::error::ErrorKind::ResponseError { .. } => todo!(),
                // mongodb::error::ErrorKind::TokioTimeoutElapsed(_) => todo!(),
                // mongodb::error::ErrorKind::RustlsConfig(_) => todo!(),
                // mongodb::error::ErrorKind::TxtLookupError { .. } => todo!(),
                // mongodb::error::ErrorKind::WaitQueueTimeoutError { .. } => todo!(),
            },
        }
    }
}

fn unique_violation_error(message: &str) -> ErrorKind {
    ErrorKind::UniqueConstraintViolation {
        constraint: match parse_unique_index_violation(message) {
            Some(index) => DatabaseConstraint::Index(index),
            None => DatabaseConstraint::CannotParse,
        },
    }
}

fn parse_unique_index_violation(message: &str) -> Option<String> {
    let re = Regex::new(r"duplicate\skey\serror\scollection:\s.*\sindex:\s(.*)\sdup\skey").unwrap();

    match re.captures(message) {
        Some(caps) => caps.get(1).map(|cap| cap.as_str().to_owned()),
        None => None,
    }
}

impl From<mongodb::bson::oid::Error> for MongoError {
    fn from(err: mongodb::bson::oid::Error) -> Self {
        match err {
            oid::Error::ArgumentError { message } => MongoError::MalformedObjectId(message),
            oid::Error::FromHexError(err) => MongoError::MalformedObjectId(format!("{}", err)),
            err => unimplemented!("Unhandled MongoDB ObjectID error {}.", err),
        }
    }
}
