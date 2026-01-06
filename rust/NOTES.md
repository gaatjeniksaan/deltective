# Implementation Notes

This Rust implementation is a port of the Python Deltective application. Some notes:

## API Compatibility

The deltalake Rust crate API may differ from what's implemented here. When compiling, you may need to adjust:

1. **File Access**: The `get_files()` method may not exist or may have a different name. Check the deltalake crate documentation for the correct method to get file information.

2. **Schema Access**: The schema API (`schema.arrow_schema()`) may need adjustment based on the actual deltalake crate version.

3. **Metadata/Protocol**: These are accessed synchronously in the code, but the actual API may require async calls.

4. **History**: The `history()` method signature may differ - it might require parameters or return a different type.

## Compilation

To compile and test:

```bash
cd rust
cargo build
```

If you encounter compilation errors related to the deltalake API, check:
- The deltalake crate version in Cargo.toml
- The actual API documentation at: https://docs.rs/deltalake/

## Azure Support

Azure storage support is stubbed out but not fully implemented. To add full Azure support:

1. Use `azure_identity` crate for authentication
2. Configure storage options properly for deltalake
3. Handle Azure-specific URL formats (abfss://)

## Testing

The code structure mirrors the Python version, so functionality should be equivalent once API compatibility issues are resolved.

