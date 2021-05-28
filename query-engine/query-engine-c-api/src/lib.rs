mod introspection;
mod engine_old;
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
use crate::engine_old::Engine;
use std::sync::Arc;
use request_handlers::{GraphQlBody, PrismaResponse};

#[no_mangle]
pub extern "C" fn hello(name: *const c_char) {
    let buf_name = unsafe { CStr::from_ptr(name).to_bytes() };
    let str_name = String::from_utf8(buf_name.to_vec()).unwrap();

    let introspection_fut = Introspection::introspect(str_name);

    let res = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(introspection_fut);

    let schema = match res {
        introspection::Result::Ok(schema) => schema,
        introspection::Result::Err(_) => String::from(""),
    };

    println!("schema\n{}", schema);
}

pub struct IntrospectionResult {
    schema: *mut c_char,
    error: *mut c_char,
}

#[no_mangle]
pub extern "C" fn prisma_introspect(schema: *const c_char) -> *const IntrospectionResult {
    let buf_schema = unsafe { CStr::from_ptr(schema).to_bytes() };
    let str_schema = String::from_utf8(buf_schema.to_vec()).unwrap();

    let introspection_fut = Introspection::introspect(str_schema);

    let res = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(introspection_fut);

    let result = match res {
        introspection::Result::Ok(schema) => {
            let str_schema = CString::new(schema).unwrap();
            let err = CString::new("").unwrap();
            IntrospectionResult {
                schema: str_schema.to_owned().into_raw(),
                error: null_mut(),
            }
        }
        introspection::Result::Err(e) => {
            let str_schema = CString::new("").unwrap();
            let err = CString::new(e.to_string()).unwrap();
            IntrospectionResult {
                schema: null_mut(),
                error: err.to_owned().into_raw(),
            }
        }
    };
    &result
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
        let schema = (*ptr).schema;
        let error = (*ptr).error;
        if !schema.is_null() {
            CString::from_raw(schema);
        }
        if !error.is_null() {
            CString::from_raw(error);
        }
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
        Err(err) => {
            println!("{}", err.to_string());
            return null();
        }
    };

    /*let engine_future = Engine::new(str_schema.clone());

    let engine = match resolve_future(engine_future) {
        Ok(engine) => engine,
        Err(_) => return null()
    };*/

    let prisma = Prisma {
        schema: str_schema,
        engine,
    };

    let b = Box::new(prisma);
    let ptr = Box::into_raw(b);

    unsafe {
        match resolve_future((*ptr).engine.connect()) {
            Ok(_) => {
                println!("engine connected")
            }
            Err(e) => {
                return null();
            }
        };
    }

    /*for n in 1..11 {
        let data = r#"
        {
            "query": "query AllUsers {findManyusers(take: 20){id name email}}",
            "variables": {}
        }"#;

        let body: GraphQlBody = serde_json::from_str(data).unwrap();

        let future = unsafe { (*ptr).engine.query(body) };
        match resolve_future(future) {
            Ok(res) => {
                let data = serde_json::to_string(&res).unwrap();
                println!("result(prisma_new:{}): {}", n, data)
            }
            Err(_) => {
                println!("result(prisma_new:{})", n)
            }
        };
    }*/

    ptr
}

#[no_mangle]
pub extern "C" fn prisma_execute(ptr: *const Prisma) -> *mut c_char {

    let data = r#"
        {
            "query": "query Messages {findManymessages(take: 20 orderBy: [{id: desc}]){id message users {id name}}}",
            "variables": {}
        }"#;

    let body: GraphQlBody = serde_json::from_str(data).unwrap();

    let engine = unsafe { (*ptr).engine.clone() };
    let future = unsafe { engine.query(body) };
    let response = match resolve_future(future) {
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

/*#[no_mangle]
pub extern "C" fn prisma_connect(ptr: *const Prisma) -> u8 {
    if ptr.is_null() {
        return 0;
    }
    let engine = unsafe { (*ptr).engine.clone() };
    let connect_future = engine.connect();
    match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(connect_future) {
        Ok(_) => return 1,
        Err(e) => {
            println!("{}", e);
            return 0;
        }
    }
}
*/
#[no_mangle]
pub extern "C" fn prisma_disconnect(ptr: *const Prisma) -> u8 {
    if ptr.is_null() {
        return 0;
    }
    let engine = unsafe { (*ptr).engine.clone() };
    resolve_future(engine.disconnect());
    1
}

#[no_mangle]
pub extern "C" fn free_prisma(ptr: *mut Prisma) {
    unsafe {
        (*ptr).engine.disconnect();
        Box::from_raw(ptr);
    }
}

#[no_mangle]
pub extern "C" fn prisma_get_schema(ptr: *const Prisma) -> *mut c_char {
    let schema = unsafe { (*ptr).schema.as_str() };
    unsafe {
        let str_schema = CString::new(schema).unwrap().to_owned();
        str_schema.into_raw()
    }
}

fn resolve_future<F: Future>(future: F) -> F::Output {
    return tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(future);
}

#[cfg(test)]
mod tests {
    use crate::{prisma_introspect, prisma_new, prisma_execute, resolve_future};
    use crate::CString;
    use crate::CStr;
    use crate::engine_old::Engine;
    use introspection_core::Error;
    use std::ptr::null;

    #[test]
    fn valid_introspection() {
        let schema = "datasource db {
		provider = \"postgresql\"
		url      = \"postgresql://admin:admin@localhost:15432/wundergraph?schema=wg\"
	}";
        let schema = CString::new(schema.to_string()).unwrap().to_owned().into_raw();
        let result = prisma_introspect(schema);
        unsafe {
            let cstring_schema = CString::from_raw((*result).schema);
            let str_schema = cstring_schema.to_str().unwrap().to_string();
            assert_eq!((*result).schema.is_null(), false);
            assert_ne!(str_schema, "")
        }
    }

    #[test]
    fn test_prisma_connect() {
        let schema = "datasource db {
		provider = \"postgresql\"
		url      = \"postgresql://admin:admin@localhost:54321/example?schema=public&connection_limit=5&pool_timeout=2\"
	}";
        unsafe {
            let schema = CString::new(schema.to_string()).unwrap().to_owned().into_raw();
            let intro = prisma_introspect(schema);
            assert_eq!((*intro).schema.is_null(), false);

            let prisma = prisma_new((*intro).schema);
            assert_eq!(prisma.is_null(), false);

            for n in 1..7 {
                let result = prisma_execute(prisma);
                assert_eq!(result.is_null(), false);

                let result_cstring = CString::from_raw(result);
                let result_str = result_cstring.to_str().unwrap().to_string();

                println!("result(prisma_execute:{}): {}", n, result_str);
            }
        }
    }
}