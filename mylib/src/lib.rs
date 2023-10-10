// This is the interface to the JVM that we'll call the majority of our
// methods on.
use jni::JNIEnv;

// These objects are what you should use as arguments to your native
// function. They carry extra lifetime information to prevent them escaping
// this context and getting used after being GC'd.
use jni::objects::{JClass, JString};

// This is just a pointer. We'll be returning it from our function. We
// can't return one of the objects with lifetime information because the
// lifetime checker won't let us.
use jni::sys::jstring;
use std::fmt::format;

// This keeps Rust from "mangling" the name and making it unique for this
// crate.

#[no_mangle]
pub extern "system" fn Java_HelloWorld_hello<'local>(
    mut env: JNIEnv<'local>,
    class: JClass<'local>,
    input: JString<'local>,
) -> jstring {
    // First, we have to get the string out of Java. Check out the `strings`
    // module for more info on how this works.
    let input: String = env
        .get_string(&input)
        .expect("Couldn't get java string")
        .into();

    // Then we have to create a new Java string to return. Again, more info
    // in the `strings` module.
    let output = env
        .new_string(format!("Hello, {}", input))
        .expect("Couldn't create java string");

    // Finally, extract the raw pointer to return.
    output.into_raw()
}
