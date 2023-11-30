//! A derive macro for the `RemoteExecute` trait.
//!
//! This construction enables operations to be serialized and executed by a
//! remote service in an opaque manner. It uses the [`linkme`](https://docs.rs/linkme)
//! crate to collect all operation execution pointers into a single slice that
//! are gathered into a contiguous section of the binary by the linker.
//!
//! # Implementation details
//!
//! A globally unique identifier is assigned to each operation. Then, a unique
//! function that handles deserialization, execution, and re-serialization of
//! the result is generated for each operation. A pointer to this function is
//! registered with the distributed slice at the index of the operation's
//! identifier. This allows execution function to be dereferenced by only the
//! operation identifier, which is serialized with the task.
//!
//! This scheme allows the tasks sent to workers to be opaque, while still
//! enabling efficient lookup and execution of the appropriate operation.
extern crate proc_macro;

use std::sync::atomic::{AtomicU8, Ordering};

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Attribute, DeriveInput, Error, Ident, Result};

static OPERATION_ID_COUNTER: AtomicU8 = AtomicU8::new(0);

/// Check if the `internal` attribute is present on the derive macro.
///
/// Quoted variables need to be slightly modified if the macro is being
/// called from the `paladin` crate itself.
fn get_is_internal(attrs: &mut Vec<Attribute>) -> Result<bool> {
    let mut is_internal = None;
    let mut errors: Option<Error> = None;

    attrs.retain(|attr| {
        if !attr.path().is_ident("paladin") {
            return true;
        }
        if let Err(err) = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("internal") {
                if is_internal.is_some() {
                    return Err(meta.error("duplicate paladin crate attribute"));
                }

                is_internal = Some(true);
                Ok(())
            } else {
                Err(meta.error("unsupported paladin attribute"))
            }
        }) {
            match &mut errors {
                None => errors = Some(err),
                Some(errors) => errors.combine(err),
            }
        }
        false
    });

    match errors {
        None => Ok(is_internal.unwrap_or(false)),
        Some(errors) => Err(errors),
    }
}

/// See the [module level documentation](crate) for more information.
#[proc_macro_derive(RemoteExecute, attributes(paladin))]
pub fn operation_derive(input: TokenStream) -> TokenStream {
    let mut input = parse_macro_input!(input as DeriveInput);

    let is_internal = match get_is_internal(&mut input.attrs) {
        Ok(path) => path,
        Err(err) => return err.to_compile_error().into(),
    };

    // The path to the `paladin` crate.
    // If the derive macro is being called from the `paladin` crate itself, then
    // the path is `crate`, otherwise it is `::paladin`.
    let paladin_path = if is_internal {
        quote! { crate }
    } else {
        quote! { ::paladin }
    };

    // If the derive macro is being called from the `paladin` crate itself, then
    // the `linkme` attribute is not needed.
    // Otherwise, we need to point the `linkme` attribute to the `paladin` so that
    // consumers of the derive macro do not need to add the `linkme` crate as a
    // dependency.
    let linkme_path_override = if is_internal {
        quote! {}
    } else {
        quote! {
            #[linkme(crate=#paladin_path::__private::linkme)]
        }
    };

    let name = &input.ident;
    let generics = &input.generics;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    let operation_id = OPERATION_ID_COUNTER.fetch_add(1, Ordering::SeqCst);
    let function_name = Ident::new(&format!("__execute_{}", operation_id), name.span());

    let expanded = quote! {
        impl #impl_generics #paladin_path::operation::RemoteExecute for #name #ty_generics #where_clause {
            const ID: u8 = #operation_id;
        }

        #[#paladin_path::__private::linkme::distributed_slice(#paladin_path::__private::OPERATIONS)]
        #linkme_path_override
        fn #function_name(task: #paladin_path::task::AnyTask) -> #paladin_path::__private::futures::future::BoxFuture<
            'static,
            #paladin_path::operation::Result<#paladin_path::task::AnyTaskOutput>
        > {
            Box::pin(async move {
                // Define the execution logic in a closure so that it can be retried if it fails.
                let get_result = || async {
                    // Deserialize the operation.
                    let op = #name::from_bytes(task.serializer, &task.op)?;

                    // Deserialize the input.
                    let input = op.input_from_bytes(task.serializer, &task.input)?;
                    #paladin_path::__private::tracing::debug!(operation = %stringify!(#name), input = ?input, "executing operation");

                    // Spawn a blocking task to execute the operation.
                    let output = #paladin_path::__private::tokio::task::spawn_blocking(move || {
                        // Execute the operation, catching panics.
                        let typed_output = std::panic::catch_unwind(::std::panic::AssertUnwindSafe(||
                            op.execute(input)
                        ))
                        // Convert panics to fatal operation errors.
                        .map_err(|e| #paladin_path::operation::FatalError::from_str(
                            &format!("operation {} panicked: {e:?}", stringify!(#name)),
                            #paladin_path::operation::FatalStrategy::Terminate
                        ))??;

                        #paladin_path::__private::tracing::debug!(operation = %stringify!(#name), output = ?typed_output, "operation executed successfully");

                        // Serialize the output.
                        let serialized_output = op.output_to_bytes(task.serializer, typed_output)?;

                        Ok(serialized_output) as #paladin_path::operation::Result<Vec<u8>>
                    })
                    .await
                    .map_err(|e| #paladin_path::operation::FatalError::new(
                        e,
                        #paladin_path::operation::FatalStrategy::Terminate
                    ))??;

                    Ok(output) as #paladin_path::operation::Result<Vec<u8>>
                };

                let result = match get_result().await {
                    Err(err) => {
                        println!("error: {:?}", err);
                        // If the operation failed, it according to the error's retry strategy.
                        err.retry_trace(get_result, |e| {
                            #paladin_path::__private::tracing::warn!(operation = %stringify!(#name), error = ?e, "transient operation failure");
                        })
                        .await
                    },
                    result => result
                };

                // Convert the typed result into an opaque task output.
                result.map(|output| #paladin_path::task::AnyTaskOutput {
                    metadata: task.metadata,
                    output,
                    serializer: task.serializer,
                })
            })
        }
    };

    TokenStream::from(expanded)
}
