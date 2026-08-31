//! Proc macros for the Rustvello distributed task library.
//!
//! Provides `#[rustvello::task]` and `#[rustvello::workflow]` attribute macros
//! that generate a [`Task`] trait implementation from a plain function.

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::{
    parse_macro_input, FnArg, GenericArgument, Ident, ItemFn, LitBool, LitInt, LitStr, Pat,
    PathArguments, ReturnType, Token, Type,
};

/// Define a Rustvello task from a function.
///
/// Generates a params struct (if the function has parameters), a task struct,
/// and a [`Task`](rustvello_core::task::Task) trait implementation.
///
/// # Basic usage
///
/// ```rust,ignore
/// #[rustvello::task]
/// fn add(x: i32, y: i32) -> i32 {
///     x + y
/// }
/// ```
///
/// This generates:
/// - `AddParams` — a serializable struct with fields `x: i32, y: i32`
/// - `AddTask` — a unit struct implementing `Task`
/// - The original `add` function is preserved for direct calls
///
/// # Configuration
///
/// ```rust,ignore
/// #[rustvello::task(max_retries = 3)]
/// fn process(data: String) -> String {
///     data.to_uppercase()
/// }
/// ```
///
/// # Fallible tasks
///
/// If the function returns `Result<T, _>` or `RustvelloResult<T>`, the
/// macro extracts `T` as the task's result type and passes the function's
/// return value through without wrapping in `Ok`.
///
/// ```rust,ignore
/// #[rustvello::task]
/// fn divide(x: f64, y: f64) -> RustvelloResult<f64> {
///     if y == 0.0 {
///         return Err(RustvelloError::runner_err("division by zero".into()));
///     }
///     Ok(x / y)
/// }
/// ```
///
/// # Supported attributes
///
/// | Attribute       | Type       | Description                                     |
/// |-----------------|------------|-------------------------------------------------|
/// | `max_retries`   | `u32`      | Maximum retry attempts (default: 0)             |
/// | `module`        | `&str`     | Override module in TaskId                        |
/// | `concurrency`   | `&str`     | Concurrency control: "unlimited"|"task"|"argument"|"none" |
/// | `registration_concurrency` | `&str` | Registration-time concurrency control    |
/// | `key_arguments` | `[&str]`   | Parameter names used as concurrency keys        |
/// | `cache_results` | `bool`     | Cache results for identical arguments            |
/// | `disable_cache_args` | `[&str]` | Arg names to exclude from cache key          |
/// | `retry_for_errors` | `[&str]` | Error type names that trigger retries            |
/// | `on_diff_non_key_args_raise` | `bool` | Raise on non-key arg mismatch          |
/// | `parallel_batch_size` | `usize` | Batch size for parallelize()                   |
/// | `reroute_on_cc` | `bool`     | Reroute when hitting concurrency limits          |
/// | `blocking`      | `bool`     | Run on a blocking thread                         |
#[proc_macro_attribute]
pub fn task(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attrs = parse_macro_input!(attr as TaskAttrs);
    let func = parse_macro_input!(item as ItemFn);

    match expand_task(attrs, func) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

/// Define an explicit workflow root or sub-workflow root.
///
/// This accepts the same configuration attributes as [`task`] and marks the
/// generated task as workflow-defining. Ordinary `#[task]` functions remain
/// standalone unless invoked from inside a workflow.
#[proc_macro_attribute]
pub fn workflow(attr: TokenStream, item: TokenStream) -> TokenStream {
    let mut attrs = parse_macro_input!(attr as TaskAttrs);
    attrs.is_workflow_task = true;
    let func = parse_macro_input!(item as ItemFn);

    match expand_task(attrs, func) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

// ---------------------------------------------------------------------------
// Attribute parsing
// ---------------------------------------------------------------------------

struct TaskAttrs {
    is_workflow_task: bool,
    max_retries: Option<u32>,
    module: Option<String>,
    concurrency: Option<String>,
    registration_concurrency: Option<String>,
    key_arguments: Option<Vec<String>>,
    cache_results: Option<bool>,
    disable_cache_args: Option<Vec<String>>,
    retry_for_errors: Option<Vec<String>>,
    on_diff_non_key_args_raise: Option<bool>,
    parallel_batch_size: Option<usize>,
    reroute_on_cc: Option<bool>,
    blocking: Option<bool>,
}

impl Parse for TaskAttrs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut max_retries = None;
        let mut module = None;
        let mut concurrency = None;
        let mut registration_concurrency = None;
        let mut key_arguments = None;
        let mut cache_results = None;
        let mut disable_cache_args = None;
        let mut retry_for_errors = None;
        let mut on_diff_non_key_args_raise = None;
        let mut parallel_batch_size = None;
        let mut reroute_on_cc = None;
        let mut blocking = None;

        while !input.is_empty() {
            let key: Ident = input.parse()?;
            input.parse::<Token![=]>()?;
            let key_str = key.to_string();

            // Helper to reject duplicate attribute keys
            macro_rules! check_dup {
                ($opt:expr) => {
                    if $opt.is_some() {
                        return Err(syn::Error::new(
                            key.span(),
                            format!("duplicate task attribute: `{}`", key_str),
                        ));
                    }
                };
            }

            match key_str.as_str() {
                "max_retries" => {
                    check_dup!(max_retries);
                    let lit: LitInt = input.parse()?;
                    max_retries = Some(lit.base10_parse()?);
                }
                "module" => {
                    check_dup!(module);
                    let lit: LitStr = input.parse()?;
                    module = Some(lit.value());
                }
                "concurrency" => {
                    check_dup!(concurrency);
                    let lit: LitStr = input.parse()?;
                    validate_concurrency_str(&lit)?;
                    concurrency = Some(lit.value());
                }
                "registration_concurrency" => {
                    check_dup!(registration_concurrency);
                    let lit: LitStr = input.parse()?;
                    validate_concurrency_str(&lit)?;
                    registration_concurrency = Some(lit.value());
                }
                "key_arguments" => {
                    check_dup!(key_arguments);
                    let content;
                    syn::bracketed!(content in input);
                    let items: Punctuated<LitStr, Token![,]> =
                        Punctuated::parse_terminated(&content)?;
                    key_arguments = Some(items.iter().map(LitStr::value).collect());
                }
                "cache_results" => {
                    check_dup!(cache_results);
                    let lit: LitBool = input.parse()?;
                    cache_results = Some(lit.value());
                }
                "reroute_on_cc" => {
                    check_dup!(reroute_on_cc);
                    let lit: LitBool = input.parse()?;
                    reroute_on_cc = Some(lit.value());
                }
                "blocking" => {
                    check_dup!(blocking);
                    let lit: LitBool = input.parse()?;
                    blocking = Some(lit.value());
                }
                "disable_cache_args" => {
                    check_dup!(disable_cache_args);
                    let content;
                    syn::bracketed!(content in input);
                    let items: Punctuated<LitStr, Token![,]> =
                        Punctuated::parse_terminated(&content)?;
                    disable_cache_args = Some(items.iter().map(LitStr::value).collect());
                }
                "retry_for_errors" => {
                    check_dup!(retry_for_errors);
                    let content;
                    syn::bracketed!(content in input);
                    let items: Punctuated<LitStr, Token![,]> =
                        Punctuated::parse_terminated(&content)?;
                    retry_for_errors = Some(items.iter().map(LitStr::value).collect());
                }
                "on_diff_non_key_args_raise" => {
                    check_dup!(on_diff_non_key_args_raise);
                    let lit: LitBool = input.parse()?;
                    on_diff_non_key_args_raise = Some(lit.value());
                }
                "parallel_batch_size" => {
                    check_dup!(parallel_batch_size);
                    let lit: LitInt = input.parse()?;
                    parallel_batch_size = Some(lit.base10_parse()?);
                }
                other => {
                    let known = [
                        "max_retries",
                        "module",
                        "concurrency",
                        "registration_concurrency",
                        "key_arguments",
                        "cache_results",
                        "disable_cache_args",
                        "retry_for_errors",
                        "on_diff_non_key_args_raise",
                        "parallel_batch_size",
                        "reroute_on_cc",
                        "blocking",
                    ];
                    let suggestion = known
                        .iter()
                        .filter(|k| {
                            // Simple edit-distance heuristic: starts with same letter
                            // or shares a long common prefix
                            k.starts_with(&other[..1.min(other.len())])
                                || other.contains(&k[..3.min(k.len())])
                                || k.contains(&other[..3.min(other.len())])
                        })
                        .copied()
                        .next();
                    let msg = match suggestion {
                        Some(s) => format!(
                            "unknown task attribute: `{other}`. Did you mean `{s}`?\n\
                             Valid attributes: {}",
                            known.join(", ")
                        ),
                        None => format!(
                            "unknown task attribute: `{other}`.\n\
                             Valid attributes: {}",
                            known.join(", ")
                        ),
                    };
                    return Err(syn::Error::new(key.span(), msg));
                }
            }

            if !input.is_empty() {
                input.parse::<Token![,]>()?;
            }
        }

        Ok(Self {
            is_workflow_task: false,
            max_retries,
            module,
            concurrency,
            registration_concurrency,
            key_arguments,
            cache_results,
            disable_cache_args,
            retry_for_errors,
            on_diff_non_key_args_raise,
            parallel_batch_size,
            reroute_on_cc,
            blocking,
        })
    }
}

fn validate_concurrency_str(lit: &LitStr) -> syn::Result<()> {
    match lit.value().as_str() {
        "unlimited" | "task" | "argument" | "none" => Ok(()),
        other => Err(syn::Error::new(
            lit.span(),
            format!(
                "invalid concurrency value: `{other}`. \
                 Expected one of: \"unlimited\", \"task\", \"argument\", \"none\""
            ),
        )),
    }
}

// ---------------------------------------------------------------------------
// Expansion
// ---------------------------------------------------------------------------

fn expand_task(attrs: TaskAttrs, func: ItemFn) -> syn::Result<proc_macro2::TokenStream> {
    validate_function(&func)?;
    validate_attrs_against_params(&attrs, &func)?;

    let fn_name = &func.sig.ident;
    let fn_name_str = fn_name.to_string();
    let vis = &func.vis;

    // PascalCase struct names
    let task_struct = format_ident!("{}Task", to_pascal_case(&fn_name_str));
    let params_struct = format_ident!("{}Params", to_pascal_case(&fn_name_str));
    let fn_name_register = format_ident!("__rustvello_register_{}", fn_name_str);

    // Crate paths — generated code references via rustvello's __private module
    let core_path = quote! { ::rustvello::__private::rustvello_core };
    let proto_path = quote! { ::rustvello::__private::rustvello_proto };
    let serde_path = quote! { ::rustvello::__private::serde };
    let serde_crate_str = "::rustvello::__private::serde";

    // Return type analysis
    let (result_type, wrap_ok) = parse_return_type(&func.sig.output)?;

    // Extract parameters
    let params = extract_params(&func)?;
    let param_names: Vec<&Ident> = params.iter().map(|(name, _)| name).collect();

    // Config builder
    let config_body = build_config(&attrs, &proto_path);

    // Module for TaskId
    let module_expr = match &attrs.module {
        Some(m) => quote! { #m },
        None => quote! { module_path!() },
    };

    // Delegating call to the original function
    let fn_call = if params.is_empty() {
        quote! { #fn_name() }
    } else {
        quote! { #fn_name(#(#param_names),*) }
    };

    let run_body = if wrap_ok {
        quote! { Ok(#fn_call) }
    } else {
        quote! { #fn_call }
    };

    let generated = if params.is_empty() {
        generate_no_params(
            &func,
            vis,
            &task_struct,
            &fn_name_register,
            &core_path,
            &proto_path,
            &module_expr,
            fn_name_str.as_str(),
            &result_type,
            &config_body,
            &run_body,
        )
    } else {
        generate_with_params(
            &func,
            vis,
            &task_struct,
            &params_struct,
            &fn_name_register,
            &core_path,
            &proto_path,
            &serde_path,
            serde_crate_str,
            &module_expr,
            fn_name_str.as_str(),
            &result_type,
            &config_body,
            &run_body,
            &params,
            &param_names,
        )
    };

    Ok(generated)
}

/// Reject unsupported function forms (async, unsafe, generic).
fn validate_function(func: &ItemFn) -> syn::Result<()> {
    if func.sig.asyncness.is_some() {
        return Err(syn::Error::new_spanned(
            &func.sig,
            "#[rustvello::task] does not support async functions yet",
        ));
    }
    if func.sig.unsafety.is_some() {
        return Err(syn::Error::new_spanned(
            &func.sig,
            "#[rustvello::task] does not support unsafe functions",
        ));
    }
    if !func.sig.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &func.sig.generics,
            "#[rustvello::task] does not support generic functions",
        ));
    }
    Ok(())
}

/// Validate key_arguments and disable_cache_args against actual function parameters.
fn validate_attrs_against_params(attrs: &TaskAttrs, func: &ItemFn) -> syn::Result<()> {
    let param_name_strs: Vec<String> = extract_params(func)?
        .iter()
        .map(|(name, _)| name.to_string())
        .collect();

    if let Some(ref keys) = attrs.key_arguments {
        for key in keys {
            if !param_name_strs.contains(key) {
                return Err(syn::Error::new(
                    func.sig.ident.span(),
                    format!(
                        "key_arguments entry `{key}` does not match any function parameter. \
                         Valid parameters: {}",
                        param_name_strs.join(", ")
                    ),
                ));
            }
        }
    }

    if let Some(ref args) = attrs.disable_cache_args {
        for arg in args {
            if !param_name_strs.contains(arg) {
                return Err(syn::Error::new(
                    func.sig.ident.span(),
                    format!(
                        "disable_cache_args entry `{arg}` does not match any function parameter. \
                         Valid parameters: {}",
                        param_name_strs.join(", ")
                    ),
                ));
            }
        }
    }
    Ok(())
}

/// Generate the task implementation for a zero-parameter function.
#[allow(clippy::too_many_arguments)]
fn generate_no_params(
    func: &ItemFn,
    vis: &syn::Visibility,
    task_struct: &Ident,
    fn_name_register: &Ident,
    core_path: &proc_macro2::TokenStream,
    proto_path: &proc_macro2::TokenStream,
    module_expr: &proc_macro2::TokenStream,
    fn_name_str: &str,
    result_type: &proc_macro2::TokenStream,
    config_body: &proc_macro2::TokenStream,
    run_body: &proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    quote! {
        #func

        #vis struct #task_struct {
            task_id: #proto_path::identifiers::TaskId,
            config: #proto_path::config::TaskConfig,
        }

        impl #task_struct {
            /// Create a new instance with the default task ID and config.
            pub fn new() -> Self {
                Self {
                    task_id: #proto_path::identifiers::TaskId::new(#module_expr, #fn_name_str),
                    config: #config_body,
                }
            }
        }

        impl Default for #task_struct {
            fn default() -> Self {
                Self::new()
            }
        }

        impl #core_path::task::Task for #task_struct {
            type Params = ();
            type Result = #result_type;

            fn task_id(&self) -> &#proto_path::identifiers::TaskId {
                &self.task_id
            }

            fn config(&self) -> &#proto_path::config::TaskConfig {
                &self.config
            }

            fn run(
                &self,
                _params: (),
            ) -> #core_path::error::RustvelloResult<#result_type> {
                #run_body
            }
        }

        fn #fn_name_register(
            registry: &mut #core_path::task::TaskRegistry,
        ) -> #core_path::error::RustvelloResult<()> {
            registry.register_typed(#task_struct::new())
        }

        ::rustvello::__private::inventory::submit! {
            ::rustvello::__private::TaskEntry {
                register_fn: #fn_name_register,
            }
        }
    }
}

/// Generate the task implementation for a function with parameters.
#[allow(clippy::too_many_arguments)]
fn generate_with_params(
    func: &ItemFn,
    vis: &syn::Visibility,
    task_struct: &Ident,
    params_struct: &Ident,
    fn_name_register: &Ident,
    core_path: &proc_macro2::TokenStream,
    proto_path: &proc_macro2::TokenStream,
    serde_path: &proc_macro2::TokenStream,
    serde_crate_str: &str,
    module_expr: &proc_macro2::TokenStream,
    fn_name_str: &str,
    result_type: &proc_macro2::TokenStream,
    config_body: &proc_macro2::TokenStream,
    run_body: &proc_macro2::TokenStream,
    params: &[(Ident, Type)],
    param_names: &[&Ident],
) -> proc_macro2::TokenStream {
    let param_fields: Vec<_> = params
        .iter()
        .map(|(name, ty)| quote! { pub #name: #ty })
        .collect();

    quote! {
        #func

        #[derive(Debug, Clone, #serde_path::Serialize, #serde_path::Deserialize)]
        #[serde(crate = #serde_crate_str)]
        #vis struct #params_struct {
            #(#param_fields,)*
        }

        #vis struct #task_struct {
            task_id: #proto_path::identifiers::TaskId,
            config: #proto_path::config::TaskConfig,
        }

        impl #task_struct {
            /// Create a new instance with the default task ID and config.
            pub fn new() -> Self {
                Self {
                    task_id: #proto_path::identifiers::TaskId::new(#module_expr, #fn_name_str),
                    config: #config_body,
                }
            }
        }

        impl Default for #task_struct {
            fn default() -> Self {
                Self::new()
            }
        }

        impl #core_path::task::Task for #task_struct {
            type Params = #params_struct;
            type Result = #result_type;

            fn task_id(&self) -> &#proto_path::identifiers::TaskId {
                &self.task_id
            }

            fn config(&self) -> &#proto_path::config::TaskConfig {
                &self.config
            }

            fn run(
                &self,
                params: #params_struct,
            ) -> #core_path::error::RustvelloResult<#result_type> {
                let #params_struct { #(#param_names),* } = params;
                #run_body
            }
        }

        fn #fn_name_register(
            registry: &mut #core_path::task::TaskRegistry,
        ) -> #core_path::error::RustvelloResult<()> {
            registry.register_typed(#task_struct::new())
        }

        ::rustvello::__private::inventory::submit! {
            ::rustvello::__private::TaskEntry {
                register_fn: #fn_name_register,
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn extract_params(func: &ItemFn) -> syn::Result<Vec<(Ident, Type)>> {
    func.sig
        .inputs
        .iter()
        .map(|arg| match arg {
            FnArg::Typed(pat_type) => {
                let name = match &*pat_type.pat {
                    Pat::Ident(pi) => pi.ident.clone(),
                    _ => {
                        return Err(syn::Error::new_spanned(
                            pat_type,
                            "expected a simple parameter name",
                        ))
                    }
                };
                Ok((name, (*pat_type.ty).clone()))
            }
            FnArg::Receiver(r) => Err(syn::Error::new_spanned(
                r,
                "#[rustvello::task] functions cannot take self",
            )),
        })
        .collect()
}

/// Parse the return type. Returns (inner_type, should_wrap_in_ok).
///
/// - `-> i32` → `(i32, true)` — body will be wrapped in `Ok(...)`
/// - `-> Result<i32, _>` → `(i32, false)` — body returned as-is
/// - `-> RustvelloResult<i32>` → `(i32, false)` — body returned as-is
/// - (no return) → `((), true)`
#[allow(clippy::unnecessary_wraps)]
fn parse_return_type(ret: &ReturnType) -> syn::Result<(proc_macro2::TokenStream, bool)> {
    match ret {
        ReturnType::Default => Ok((quote! { () }, true)),
        ReturnType::Type(_, ty) => {
            if let Some(inner) = unwrap_result_type(ty) {
                Ok((quote! { #inner }, false))
            } else {
                Ok((quote! { #ty }, true))
            }
        }
    }
}

/// If the type looks like `Result<T, _>` or `RustvelloResult<T>`, extract T.
fn unwrap_result_type(ty: &Type) -> Option<&Type> {
    let Type::Path(tp) = ty else { return None };
    let last = tp.path.segments.last()?;
    let name = last.ident.to_string();
    if name != "Result" && name != "RustvelloResult" {
        return None;
    }
    let PathArguments::AngleBracketed(args) = &last.arguments else {
        return None;
    };
    match args.args.first()? {
        GenericArgument::Type(inner) => Some(inner),
        _ => None,
    }
}

fn build_config(
    attrs: &TaskAttrs,
    proto_path: &proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    let base = quote! { let mut config = #proto_path::config::TaskConfig::default(); };
    let mut setters = Vec::new();

    if let Some(retries) = attrs.max_retries {
        setters.push(quote! { config.max_retries = #retries; });
    }

    if let Some(ref cc) = attrs.concurrency {
        let variant = concurrency_variant(cc);
        setters.push(quote! {
            config.concurrency_control = #proto_path::status::ConcurrencyControlType::#variant;
        });
    }

    if let Some(ref rc) = attrs.registration_concurrency {
        let variant = concurrency_variant(rc);
        setters.push(quote! {
            config.registration_concurrency = #proto_path::status::ConcurrencyControlType::#variant;
        });
    }

    if let Some(ref keys) = attrs.key_arguments {
        setters.push(quote! {
            config.key_arguments = vec![#(#keys.to_string()),*];
        });
    }

    if let Some(cache) = attrs.cache_results {
        setters.push(quote! { config.cache_results = #cache; });
    }

    if let Some(reroute) = attrs.reroute_on_cc {
        setters.push(quote! { config.reroute_on_cc = #reroute; });
    }

    if let Some(blocking) = attrs.blocking {
        setters.push(quote! { config.blocking = #blocking; });
    }

    if attrs.is_workflow_task {
        setters.push(quote! {
            config.is_workflow_task = true;
            config.blocking = true;
        });
    }

    if let Some(ref args) = attrs.disable_cache_args {
        setters.push(quote! {
            config.disable_cache_args = vec![#(#args.to_string()),*];
        });
    }

    if let Some(ref errors) = attrs.retry_for_errors {
        setters.push(quote! {
            config.retry_for_errors = vec![#(#errors.to_string()),*];
        });
    }

    if let Some(raise) = attrs.on_diff_non_key_args_raise {
        setters.push(quote! { config.on_diff_non_key_args_raise = #raise; });
    }

    if let Some(batch) = attrs.parallel_batch_size {
        setters.push(quote! { config.parallel_batch_size = #batch; });
    }

    quote! {
        {
            #base
            #(#setters)*
            config
        }
    }
}

fn concurrency_variant(s: &str) -> proc_macro2::TokenStream {
    match s {
        "unlimited" => quote! { Unlimited },
        "task" => quote! { Task },
        "argument" => quote! { Argument },
        "none" => quote! { None },
        _ => unreachable!("validated in parse"),
    }
}

fn to_pascal_case(s: &str) -> String {
    s.split('_')
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                None => String::new(),
                Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
            }
        })
        .collect()
}
