#![feature(once_cell)]

mod introspection;
mod context;
mod error;
mod engine;

extern crate libc;

use libc::c_char;
use std::ffi::CStr;
use std::ffi::CString;
use std::ops::Deref;
use crate::introspection::Introspection;
use std::ptr::{null, null_mut};
use introspection_core::Error;
use std::future::Future;
use crate::engine::{QueryEngine, TelemetryOptions};
use crate::error::ApiError;
use std::sync::Arc;
use request_handlers::{GraphQlBody, PrismaResponse};
use tokio::runtime::Runtime;
use once_cell::sync::Lazy;

static RUNTIME: Lazy<Runtime> = Lazy::new(|| Runtime::new().unwrap());

pub struct IntrospectionResult {
    schema: *mut c_char,
    sdl: *mut c_char,
    error: *mut c_char,
}

#[no_mangle]
pub extern "C" fn prisma_introspect(schema: *const c_char) -> *const IntrospectionResult {
    let buf_schema = unsafe { CStr::from_ptr(schema).to_bytes() };
    let str_schema = String::from_utf8(buf_schema.to_vec()).unwrap();

    let introspection_fut = Introspection::introspect(str_schema);

    let res = RUNTIME.block_on(introspection_fut);

    let result = match res {
        introspection::Result::Ok(schema) => {
            let engine = match QueryEngine::new(schema.clone()) {
                Ok(engine) => engine,
                Err(e) => {
                    let error_cstring = CString::new(e.to_string()).unwrap();
                    return &IntrospectionResult {
                        sdl: null_mut(),
                        schema: null_mut(),
                        error: error_cstring.to_owned().into_raw(),
                    };
                }
            };
            match RUNTIME.block_on(engine.connect()) {
                Ok(_) => {}
                Err(_) => {
                    let error_cstring = CString::new(String::from("unable to connect engine")).unwrap();
                    return &IntrospectionResult {
                        schema: null_mut(),
                        sdl: null_mut(),
                        error: error_cstring.to_owned().into_raw(),
                    };
                }
            };
            let sdl = match RUNTIME.block_on(engine.sdl_schema()) {
                Ok(sdl) => sdl,
                Err(e) => {
                    let error_cstring = CString::new(String::from("sdl_schema error")).unwrap();
                    return &IntrospectionResult {
                        schema: null_mut(),
                        sdl: null_mut(),
                        error: error_cstring.to_owned().into_raw(),
                    };
                }
            };
            RUNTIME.block_on(engine.disconnect());
            let str_schema = CString::new(schema.clone()).unwrap();
            let str_sdl = CString::new(sdl.clone()).unwrap();
            IntrospectionResult {
                schema: str_schema.to_owned().into_raw(),
                sdl: str_sdl.to_owned().into_raw(),
                error: null_mut(),
            }
        }
        introspection::Result::Err(e) => {
            let err = CString::new(e.to_string()).unwrap();
            IntrospectionResult {
                schema: null_mut(),
                sdl: null_mut(),
                error: err.to_owned().into_raw(),
            }
        }
    };
    let b = Box::new(result);
    Box::into_raw(b)
}

#[no_mangle]
pub extern "C" fn free_cstring(s: *mut c_char) {
    unsafe {
        if s.is_null() {
            return;
        }
        CString::from_raw(s);
    };
    return;
}

#[no_mangle]
pub extern "C" fn free_introspection_result(ptr: *const IntrospectionResult) {
    unsafe {
        if ptr.is_null() {
            return
        }
        let schema = (*ptr).schema;
        let sdl = (*ptr).sdl;
        let error = (*ptr).error;
        if !schema.is_null() {
            CString::from_raw(schema);
        }
        if !error.is_null() {
            CString::from_raw(error);
        }
        if !sdl.is_null() {
            CString::from_raw(sdl);
        }
        let ptr = ptr as *mut IntrospectionResult;
        Box::from_raw(ptr)
    };
}

pub struct Prisma {
    schema: String,
    engine: QueryEngine,
}

impl Clone for Prisma {
    fn clone(&self) -> Self {
        Prisma {
            engine: self.engine.clone(),
            schema: self.schema.clone(),
        }
    }
}

#[no_mangle]
pub extern "C" fn prisma_new(schema: *const c_char) -> *const Prisma {
    let buf_schema = unsafe { CStr::from_ptr(schema).to_bytes() };
    let str_schema = String::from_utf8(buf_schema.to_vec()).unwrap();

    let engine = match QueryEngine::new(str_schema.clone()) {
        Ok(engine) => engine,
        Err(_) => {
            return null();
        }
    };

    match RUNTIME.block_on(engine.connect()) {
        Ok(_) => {}
        Err(_) => return null()
    };

    let prisma = Prisma {
        schema: str_schema,
        engine,
    };

    let b = Box::new(prisma);
    Box::into_raw(b)
}

#[no_mangle]
pub extern "C" fn prisma_execute(ptr: *const Prisma, query: *const c_char) -> *mut c_char {
    let engine = unsafe { (*ptr).engine.clone() };
    let query_buf = unsafe { CStr::from_ptr(query).to_bytes() };
    let query_str = String::from_utf8(query_buf.to_vec()).unwrap();
    let body: GraphQlBody = serde_json::from_str(&query_str).unwrap();
    let future = unsafe { engine.query(body) };
    let response = match RUNTIME.block_on(future) {
        Ok(res) => serde_json::to_string(&res).unwrap(),
        Err(e) => match e {
            ApiError::Conversion(e, e2) => e2,
            ApiError::Configuration(e) => e.to_string(),
            ApiError::Core(e) => String::from("core error"),
            ApiError::Connector(e) => String::from("connector error"),
            ApiError::AlreadyConnected => String::from("already connected"),
            ApiError::NotConnected => String::from("not connected"),
            ApiError::JsonDecode(e) => e,
        }
    };
    unsafe {
        let str_schema = CString::new(response).unwrap().to_owned();
        str_schema.into_raw()
    }
}

#[no_mangle]
pub extern "C" fn free_prisma(ptr: *const Prisma) {
    unsafe {
        let engine = unsafe { (*ptr).engine.clone() };
        RUNTIME.block_on(engine.disconnect());
        let ptr = ptr as *mut Prisma;
        Box::from_raw(ptr);
    }
}

#[no_mangle]
pub extern "C" fn prisma_get_schema(ptr: *const Prisma) -> *mut c_char {
    let schema = unsafe { (*ptr).schema.clone() };
    unsafe {
        let str_schema = CString::new(schema.as_str()).unwrap().to_owned();
        str_schema.into_raw()
    }
}

#[cfg(test)]
mod tests {
    use crate::{prisma_introspect, prisma_new, prisma_execute, free_prisma, free_introspection_result};
    use crate::CString;
    use crate::CStr;
    use crate::Prisma;
    use introspection_core::Error;
    use std::ptr::null;

    #[test]
    fn valid_introspection() {
        let schema = "datasource db {
		provider = \"postgresql\"
		url      = \"postgresql://admin:admin@localhost:54321/example?schema=public&connection_limit=5&pool_timeout=2\"
	}";
        let schema = CString::new(schema.to_string()).unwrap().to_owned().into_raw();
        let result = prisma_introspect(schema);
        unsafe {
            assert_eq!((*result).schema.is_null(), false);
            assert_eq!((*result).sdl.is_null(), false);
            assert_eq!((*result).error.is_null(), true);
            free_introspection_result(result);
        }
    }

    #[test]
    fn introspect_sqlserver() {
        let schema = "datasource db {
		provider = \"sqlserver\"
		url      = \"sqlserver://sa:mySecret_Password@localhost:1433/wundergraph?schema=wg\"
	}";
        let schema = CString::new(schema.to_string()).unwrap().to_owned().into_raw();
        let result = prisma_introspect(schema);
        unsafe {
            assert_eq!((*result).schema.is_null(), false);
            assert_eq!((*result).sdl.is_null(), false);
            assert_eq!((*result).error.is_null(), true);
            free_introspection_result(result);
        }
    }

    #[test]
    fn test_prisma_connect() {
        let schema = "datasource db {
		provider = \"postgresql\"
		url      = \"postgresql://admin:admin@localhost:54321/example?schema=public&connection_limit=5&pool_timeout=2\"
	}";
        let data = r#"
        {
            "query": "query Messages {findManymessages(take: 20 orderBy: [{id: desc}]){id message users {id name}}}",
            "variables": {}
        }"#;
        unsafe {
            let schema = CString::new(schema.to_string()).unwrap().to_owned().into_raw();
            let intro = prisma_introspect(schema);
            assert_eq!((*intro).schema.is_null(), false);

            let prisma = prisma_new((*intro).schema);
            assert_eq!(prisma.is_null(), false);

            for n in 1..7 {
                let query_cstring = CString::new(data.clone()).unwrap().to_owned().into_raw();
                let result = prisma_execute(prisma, query_cstring);
                assert_eq!(result.is_null(), false);

                let result_cstring = CString::from_raw(result);
                let result_str = result_cstring.to_str().unwrap().to_string();

                println!("result(prisma_execute:{}): {}", n, result_str);
            }

            free_prisma(prisma)
        }
    }
}