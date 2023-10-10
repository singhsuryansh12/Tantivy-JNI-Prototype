use std::path::{Path, PathBuf};

// This is the interface to the JVM that we'll call the majority of our
// methods on.
use jni::JNIEnv;

// These objects are what you should use as arguments to your native
// function. They carry extra lifetime information to prevent them escaping
// this context and getting used after being GC'd.
use jni::objects::{JClass, JString};
use jni::sys::jint;

// This is just a pointer. We'll be returning it from our function. We
// can't return one of the objects with lifetime information because the
// lifetime checker won't let us.
use crate::tantivy::build_index;

// This keeps Rust from "mangling" the name and making it unique for this
// crate.

pub mod tantivy;

#[no_mangle]
pub extern "system" fn Java_SearchTantivy_buildindex<'local>(
    mut env: JNIEnv<'local>,
    class: JClass<'local>,
    output_dir: JString<'local>,
    index_delete_pct: jint,
) {
    // First, we have to get the string out of Java. Check out the `strings`
    // module for more info on how this works.
    let output_dir: String = String::from(env
        .get_string(&output_dir)
        .expect("Couldn't get java string"));

    let _ = build_index(&PathBuf::from(output_dir), index_delete_pct);

}



