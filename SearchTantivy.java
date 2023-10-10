class SearchTantivy {
    // This declares that the static `hello` method will be provided
    // a native library.
    private static native void buildindex(String output_dir, int idx_del_pct);
    private static native void doquery(String input_dir, String query_field);

    static {
        // This actually loads the shared object that we'll be creating.
        // The actual location of the .so or .dll may differ based on your
        // platform.
    System.out.println(System.getProperty("java.library.path"));
    // System.setProperty("java.library.path", "./mylib/target/debug");
    System.load("/Volumes/workplace/TantivyHackathon/Tantivy-JNI-Prototype/mylib/target/debug/libmylib.dylib");
    }

    // The rest is just regular ol' Java!
    public static void main(String[] args) {
        SearchTantivy.buildindex("./idx", 2);
        SearchTantivy.doquery("./idx", "TOP_10_COUNT\tplayer\n");
    }
}