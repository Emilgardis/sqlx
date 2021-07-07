use proc_macro2::{Span, TokenStream};
use quote::{quote, ToTokens};
use syn::{Data, DataStruct, DeriveInput, Field, Fields, FieldsNamed, FieldsUnnamed, Lifetime, Stmt, Type, parse_quote, punctuated::Punctuated, spanned::Spanned, token::Comma};

use crate::derives::attributes::SqlxChildAttributes;

use super::{
    attributes::{parse_child_attributes, parse_container_attributes},
    rename_all,
};

pub fn expand_derive_from_row(input: &DeriveInput) -> syn::Result<TokenStream> {
    match &input.data {
        Data::Struct(DataStruct {
            fields: Fields::Named(FieldsNamed { named, .. }),
            ..
        }) => expand_derive_from_row_struct(input, named),

        Data::Struct(DataStruct {
            fields: Fields::Unnamed(FieldsUnnamed { unnamed, .. }),
            ..
        }) => expand_derive_from_row_struct_unnamed(input, unnamed),

        Data::Struct(DataStruct {
            fields: Fields::Unit,
            ..
        }) => Err(syn::Error::new_spanned(
            input,
            "unit structs are not supported",
        )),

        Data::Enum(_) => Err(syn::Error::new_spanned(input, "enums are not supported")),

        Data::Union(_) => Err(syn::Error::new_spanned(input, "unions are not supported")),
    }
}

fn expand_derive_from_row_struct(
    input: &DeriveInput,
    fields: &Punctuated<Field, Comma>,
) -> syn::Result<TokenStream> {
    let ident = &input.ident;
    let fields: Vec<(_, SqlxChildAttributes)> = fields
        .iter()
        .map(|field| -> Result<_, syn::Error> {
            Ok((field, parse_child_attributes(&field.attrs)?))
        })
        .collect::<Result<_, _>>()?;
    let generics = &input.generics;

    let (lifetime, provided) = generics
        .lifetimes()
        .next()
        .map(|def| (def.lifetime.clone(), false))
        .unwrap_or_else(|| (Lifetime::new("'a", Span::call_site()), true));

    let (_, ty_generics, _) = generics.split_for_impl();

    let mut generics = generics.clone();
    generics.params.insert(0, parse_quote!(R: ::sqlx::Row));

    if provided {
        generics.params.insert(0, parse_quote!(#lifetime));
    }

    let predicates = &mut generics.make_where_clause().predicates;

    predicates.push(parse_quote!(&#lifetime ::std::primitive::str: ::sqlx::ColumnIndex<R>));

    for (field, ref attributes) in &fields {
        if let Some(decode_as) = &attributes.decode_as {
            let ty = &decode_as.decode_as_type;
            predicates.push(parse_quote!(#ty: ::sqlx::decode::Decode<#lifetime, R::Database>));
            predicates.push(parse_quote!(#ty: ::sqlx::types::Type<R::Database>));
        } else {
            let ty = &field.ty;
            predicates.push(parse_quote!(#ty: ::sqlx::decode::Decode<#lifetime, R::Database>));
            predicates.push(parse_quote!(#ty: ::sqlx::types::Type<R::Database>));
        }
    }

    let (impl_generics, _, where_clause) = generics.split_for_impl();

    let container_attributes = parse_container_attributes(&input.attrs)?;

    let reads = fields
        .iter()
        .filter_map(|(field, attributes)| -> Option<syn::Result<Stmt>> {
            let id = &field.ident.as_ref()?;
            let id_s = attributes
                .rename
                .to_owned()
                .or_else(|| Some(id.to_string().trim_start_matches("r#").to_owned()))
                .map(|s| match container_attributes.rename_all {
                    Some(pattern) => rename_all(&s, pattern),
                    None => s.to_string(),
                })?;

            let ty = &field.ty;

            let try_get: syn::Expr = if let Some(ref decode_as) = attributes.decode_as {
                let decode_as_type = &decode_as.decode_as_type;
                let function = &decode_as.function;
                if decode_as.fallible {
                    // Assert the return type of the function, does not produce a desirable compiler error
                    let assert_types = quote::quote_spanned!(function.span()=> 
                        fn _AssertFunction<E: ::std::error::Error + 'static + Send + Sync>() -> impl ::std::ops::Fn(#decode_as_type) -> ::std::result::Result<#ty,E> {#function}
                        );
                    parse_quote!({
                        #assert_types
                        let t: ::sqlx::Result<#decode_as_type> = row.try_get(#id_s);
                        { 
                            match t {
                                Ok(t) => #function(t).map(::std::boxed::Box::new).map(::sqlx::Error::Decode),
                                e => e, 
                            }}
                        })
                } else {
                    parse_quote!({
                        let t: ::sqlx::Result<#decode_as_type> = row.try_get(#id_s);
                            match t {
                                Ok(t) => ::std::result::Result::Ok(#function(t)),
                                Err(e) => ::std::result::Result::Err(e), 
                            }
                        })
                } 
            } else {
                parse_quote!(row.try_get(#id_s))
            };

            if attributes.default {
                Some(Ok(parse_quote!(#try_get.or_else(|e| match e {
                ::sqlx::Error::ColumnNotFound(_) => {
                    ::std::result::Result::Ok(Default::default())
                },
                e => ::std::result::Result::Err(e)
            })?;)))
            } else {
                Some(Ok(parse_quote!(
                    let #id: #ty = #try_get?;
                )))
            }
        });
    let _ = reads.clone().try_for_each(|s| s.map(|_|()))?;
    let reads = reads.filter_map(|r| r.ok());
    let names = fields.iter().map(|(field, _)| &field.ident);

    Ok(quote!(
        #[automatically_derived]
        impl #impl_generics ::sqlx::FromRow<#lifetime, R> for #ident #ty_generics #where_clause {
            fn from_row(row: &#lifetime R) -> ::sqlx::Result<Self> {
                #(#reads)*

                ::std::result::Result::Ok(#ident {
                    #(#names),*
                })
            }
        }
    ))
}

fn expand_derive_from_row_struct_unnamed(
    input: &DeriveInput,
    fields: &Punctuated<Field, Comma>,
) -> syn::Result<TokenStream> {
    let ident = &input.ident;

    let generics = &input.generics;

    let (lifetime, provided) = generics
        .lifetimes()
        .next()
        .map(|def| (def.lifetime.clone(), false))
        .unwrap_or_else(|| (Lifetime::new("'a", Span::call_site()), true));

    let (_, ty_generics, _) = generics.split_for_impl();

    let mut generics = generics.clone();
    generics.params.insert(0, parse_quote!(R: ::sqlx::Row));

    if provided {
        generics.params.insert(0, parse_quote!(#lifetime));
    }

    let predicates = &mut generics.make_where_clause().predicates;

    predicates.push(parse_quote!(
        ::std::primitive::usize: ::sqlx::ColumnIndex<R>
    ));

    for field in fields {
        let ty = &field.ty;

        predicates.push(parse_quote!(#ty: ::sqlx::decode::Decode<#lifetime, R::Database>));
        predicates.push(parse_quote!(#ty: ::sqlx::types::Type<R::Database>));
    }

    let (impl_generics, _, where_clause) = generics.split_for_impl();

    let gets = fields
        .iter()
        .enumerate()
        .map(|(idx, _)| quote!(row.try_get(#idx)?));

    Ok(quote!(
        #[automatically_derived]
        impl #impl_generics ::sqlx::FromRow<#lifetime, R> for #ident #ty_generics #where_clause {
            fn from_row(row: &#lifetime R) -> ::sqlx::Result<Self> {
                ::std::result::Result::Ok(#ident (
                    #(#gets),*
                ))
            }
        }
    ))
}
