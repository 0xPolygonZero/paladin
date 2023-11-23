//! A derive macro for the `OpKind` trait.
//!
//! Implementing types MUST be an enum. More specifically, the enum variants
//! MUST be single tuple structs, where the tuple is the type of the operation.
//! For example:
//!
//! ```
//! struct OpA;
//! struct OpB;
//! struct OpC;
//!
//! enum MyOps {
//!     OpA(OpA),
//!     OpB(OpB),
//!     OpC(OpC),
//! }
//! ```
//!
//! This is what enables a pattern match on the enum to extract the operation.
//!
//! This construction enables operations to be serialized and executed by a
//! remote service in an opaque manner. In particular, the remote service need
//! not know the exact type of operation, but rather, only the _kind_ of
//! possible operations. This macro takes care of generating the necessary code
//! to facilitate this.
//!
//! One should consolidate all operations into a single enum, which serves
//! as the registry of all available operations. The registry should be
//! exhaustive, in the sense that all possible operations one would like to be
//! able to execute should be included. This is because the `Runtime` is
//! specialized to a single `OpKind` type, and thus, can only execute operations
//! defined therein. Of course, you're free to instantiate multiple `Runtime`
//! instances to manage different sets of operations.
//!
//! # Implementation details
//!
//! This macro does a few things:
//! 1. It implements the `OpKind` trait for the enum.
//! 2. It implements `From` for each variant of the enum, allowing you to
//!    convert from the operation type to the enum.
//! 3. It implements `RemoteExecute` for `AnyTask` where the `OpKind` is the
//!    enum.
//! This is perhaps the most important part of the macro, as it is what enables
//! remote execution of operations. It takes care of deserializing the input,
//! executing the operation, serializing the output, and sending the result back
//! to the receiving channel.
extern crate proc_macro;

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields, Ident, Type};

fn parse_type_arguments(path_arguments: &syn::PathArguments) -> Vec<syn::Type> {
    match path_arguments {
        syn::PathArguments::None => Vec::new(),
        syn::PathArguments::AngleBracketed(angle_bracketed_args) => angle_bracketed_args
            .args
            .iter()
            .filter_map(|arg| {
                if let syn::GenericArgument::Type(ty) = arg {
                    Some(ty.clone())
                } else {
                    None
                }
            })
            .collect(),
        syn::PathArguments::Parenthesized(_) => Vec::new(),
    }
}

/// See the [module level documentation](crate) for more information.
#[proc_macro_derive(OpKind)]
pub fn op_kind_derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let name = &input.ident;
    // Extract the variant names and their types for the enum
    let variants: Vec<(Ident, Ident, Vec<Type>)> = if let Data::Enum(data_enum) = &input.data {
        data_enum
            .variants
            .iter()
            .map(|variant| {
                let variant_name = &variant.ident;
                if let Fields::Unnamed(fields) = &variant.fields {
                    if let Some(field) = fields.unnamed.first() {
                        if let syn::Type::Path(type_path) = &field.ty {
                            let type_name = &type_path.path.segments.first().unwrap().ident;
                            let generics = parse_type_arguments(
                                &type_path.path.segments.first().unwrap().arguments,
                            );

                            return (variant_name.clone(), type_name.clone(), generics);
                        }
                    }
                }
                panic!("OpKind expects variants to have a single unnamed field");
            })
            .collect()
    } else {
        panic!("OpKind can only be derived for enums");
    };

    let match_arms = variants
        .iter()
        .map(|(variant_name, _, _)| {
            quote! {
                #name::#variant_name(ref op) => {
                    let input = op.input_from_bytes(self.serializer, &self.input)?;

                    ::paladin::tracing::debug!("executing operation with input: {input:?}");

                    let output = std::panic::catch_unwind(||
                        op.execute(input)
                    )
                    .map_err(|_| ::paladin::operation::FatalError::from_str(
                        &format!("operation panicked"),
                        ::paladin::operation::FatalStrategy::Terminate
                    ))??;

                    ::paladin::tracing::debug!("operation executed successfully: {:?}", output);

                    let serialized_output = op.output_to_bytes(self.serializer, output)?;
                    let serialized_op = op.as_bytes(self.serializer)?;

                    let response = ::paladin::task::AnyTaskOutput {
                        metadata: self.metadata.clone(),
                        op: serialized_op,
                        output: serialized_output,
                        serializer: self.serializer,
                    };

                    Ok(response)
                }
            }
        })
        .collect::<Vec<_>>();

    // Generate the From implementations
    let impls = variants
        .iter()
        .map(|(variant_name, type_name, generics)| {
            if generics.is_empty() {
                quote! {
                    impl From<#type_name> for #name {
                        fn from(op: #type_name) -> Self {
                            #name::#variant_name(op)
                        }
                    }
                }
            } else {
                let generics_ident = generics
                    .iter()
                    .map(|generic| {
                        if let syn::Type::Path(type_path) = generic {
                            &type_path.path.segments.first().unwrap().ident
                        } else {
                            panic!("Expected a TypePath");
                        }
                    })
                    .collect::<Vec<_>>();
                quote! {
                    impl From<#type_name<#(#generics_ident),*>> for #name {
                        fn from(op: #type_name<#(#generics_ident),*>) -> Self {
                            #name::#variant_name(op)
                        }
                    }
                }
            }
        })
        .collect::<Vec<_>>();

    let expanded = quote! {
        impl ::paladin::operation::OpKind for #name {}

        #(#impls)*

        #[::paladin::async_trait]
        impl ::paladin::task::RemoteExecute<#name> for ::paladin::task::AnyTask<#name> {
            async fn remote_execute(&self, runtime: &::paladin::runtime::WorkerRuntime<#name>) -> ::paladin::operation::Result<::paladin::task::AnyTaskOutput> {
                let get_result = || async {
                    (match self.op_kind {
                        #(#match_arms)*,
                    }) as ::paladin::operation::Result<::paladin::task::AnyTaskOutput>
                };

                match get_result().await {
                    Err(err) => {
                        err.retry_trace(get_result, |e| {
                            ::paladin::tracing::warn!("transient operation failure {e:?}");
                        })
                        .await
                    },
                    result => result
                }
            }
        }
    };

    TokenStream::from(expanded)
}
