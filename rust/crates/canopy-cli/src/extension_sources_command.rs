use canopy_core::extension_marketplace_fetch::{
    MarketplaceFetchOptions, ReqwestMarketplaceTransport,
};
use canopy_core::extension_marketplace_source_service::MarketplaceSourceService;
use canopy_core::extension_source_projection::sanitize_display;
use canopy_core::extension_source_store::{ExtensionSource, ExtensionSourceStore};
use canopy_core::extensions::redact_url_credentials;
use canopy_core::services::image_generation::SystemAddressResolver;
use canopy_core::storage::Storage;
use chrono::{SecondsFormat, Utc};
use serde_json::Value;

const USAGE: &str =
    "Usage: canopy extensions sources <add <source>|remove <name>|list|update <name>>";

fn source_store() -> ExtensionSourceStore {
    ExtensionSourceStore::new(Storage::get_user_extensions_dir().join("marketplaces.json"))
}

fn marketplace_service<'a>(
    store: &'a ExtensionSourceStore,
) -> MarketplaceSourceService<'a, SystemAddressResolver, ReqwestMarketplaceTransport> {
    MarketplaceSourceService::new(
        store,
        &SystemAddressResolver,
        &ReqwestMarketplaceTransport,
        MarketplaceFetchOptions::default(),
    )
}

fn github_token() -> Option<String> {
    std::env::var("GITHUB_TOKEN")
        .ok()
        .filter(|token| !token.is_empty())
}

fn display_source(source: &ExtensionSource) -> String {
    let mut output = format!(
        "{}\n Source: {} (Type: {})",
        sanitize_display(&source.name),
        sanitize_display(&redact_url_credentials(&source.source)),
        source.source_type.as_str(),
    );
    if let Some(updated) = source
        .last_updated_at
        .as_deref()
        .or(source.added_at.as_deref())
    {
        output.push_str(&format!("\n Last updated: {}", sanitize_display(updated)));
    }
    output
}

fn list_sources() {
    let sources = source_store().read();
    if sources.is_empty() {
        println!("No marketplace sources added yet.");
        return;
    }
    println!(
        "{}",
        sources
            .iter()
            .map(display_source)
            .collect::<Vec<_>>()
            .join("\n\n")
    );
}

fn remove_source(name: &str) -> Result<(), String> {
    if !source_store()
        .remove(name)
        .map_err(|error| format!("Could not update marketplace source registry: {error}"))?
    {
        return Err(format!(
            "Marketplace \"{}\" not found.",
            sanitize_display(name)
        ));
    }
    println!("Removed marketplace \"{}\".", sanitize_display(name));
    Ok(())
}

async fn add_source(source: &str) -> Result<(), String> {
    if source.trim().is_empty() {
        return Err("Marketplace source cannot be empty.".to_owned());
    }
    let store = source_store();
    let service = marketplace_service(&store);
    let added = service
        .add_source(source, github_token().as_deref(), None)
        .await
        .map_err(|error| redact_url_credentials(&error.to_string()))?;
    println!(
        "Added marketplace \"{}\".",
        sanitize_display(&added.source.name)
    );
    Ok(())
}

async fn update_source(name: &str) -> Result<(), String> {
    let store = source_store();
    let existing = store
        .read()
        .into_iter()
        .find(|source| source.name == name)
        .ok_or_else(|| format!("Marketplace \"{}\" not found.", sanitize_display(name)))?;

    let config = marketplace_service(&store)
        .load_source(&existing.source, github_token().as_deref(), None)
        .await
        .map_err(|error| redact_url_credentials(&error.to_string()))?
        .ok_or_else(|| "Could not load this marketplace.".to_owned())?;

    // The TypeScript update command advances only lastUpdatedAt and preserves
    // the registry name, source, type, and addedAt fields.
    let updated = ExtensionSource {
        last_updated_at: Some(Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)),
        ..existing
    };
    store
        .add(&updated)
        .map_err(|error| format!("Could not update marketplace source registry: {error}"))?;

    println!("Updated marketplace \"{}\".", sanitize_display(name));
    let plugin_count = config
        .get("plugins")
        .and_then(Value::as_array)
        .map_or(0, Vec::len);
    println!("{plugin_count} available extensions");
    Ok(())
}

fn command_help() {
    println!("{USAGE}");
    println!("  add <source>   Add and load a Claude-format marketplace source");
    println!("  remove <name>  Remove a configured marketplace source");
    println!("  list           List configured marketplace sources");
    println!("  update <name>  Refresh a source's marketplace listing");
}

pub fn run(args: &[String]) -> Result<(), String> {
    if args
        .iter()
        .any(|argument| matches!(argument.as_str(), "--help" | "-h"))
    {
        command_help();
        return Ok(());
    }
    if args.first().map(String::as_str) != Some("sources") {
        return Err(format!(
            "{USAGE}\nOnly the sources subcommand is available in the Rust CLI."
        ));
    }
    let Some(command) = args.get(1).map(String::as_str) else {
        command_help();
        return Ok(());
    };

    match command {
        "list" if args.len() == 2 => {
            list_sources();
            Ok(())
        }
        "add" if args.len() == 3 => run_async(add_source(&args[2])),
        "remove" if args.len() == 3 => remove_source(&args[2]),
        "update" if args.len() == 3 => run_async(update_source(&args[2])),
        "add" | "remove" | "update" => {
            Err(format!("{USAGE}\n{command} requires exactly one argument."))
        }
        "list" => Err(format!("{USAGE}\nlist accepts no arguments.")),
        _ => Err(format!("{USAGE}\nUnknown sources command: {command}")),
    }
}

fn run_async<F>(future: F) -> Result<(), String>
where
    F: std::future::Future<Output = Result<(), String>>,
{
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| format!("could not start async runtime: {error}"))?
        .block_on(future)
}
