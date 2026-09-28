use canopy_external_context::{SERVER_NAME, SERVER_VERSION, load_config, run_stdio};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args
        .iter()
        .any(|arg| matches!(arg.as_str(), "--version" | "-V"))
    {
        println!("{SERVER_NAME} {SERVER_VERSION}");
        return;
    }
    if args
        .iter()
        .any(|arg| matches!(arg.as_str(), "--help" | "-h"))
    {
        println!(
            "{SERVER_NAME} {SERVER_VERSION}\n\nRuns the external-context MCP server over newline-delimited JSON-RPC on stdin/stdout.\nSet QWEN_EXTERNAL_CONTEXT_CONFIG to an absolute version 1 config file."
        );
        return;
    }

    let config = match load_config() {
        Ok(config) => config,
        Err(error) => {
            eprintln!("[external-context] {error}");
            std::process::exit(1);
        }
    };
    if config.version != 1 {
        eprintln!(
            "[external-context] External context MCP server requires a version 1 configuration."
        );
        std::process::exit(1);
    }
    if let Err(error) = run_stdio(config).await {
        eprintln!("[external-context] External context startup failed.");
        let _ = error;
        std::process::exit(1);
    }
}
