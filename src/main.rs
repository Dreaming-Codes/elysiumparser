use clap::Parser;
use elysiumparser::{
    add_search_with_expression, run_parser, BooleanExpression, ParserConfig,
};
use std::io::{stdout, Write};

#[derive(Parser)]
#[command(author, version, about = "Log file parser")]
struct Cli {
    /// Directory containing log files to parse
    #[arg(short, long, default_value = "logs/parser")]
    log_folder: String,

    /// Output log file path
    #[arg(short, long, default_value = "logs/parser/output.log")]
    output_log: String,

    /// Filter for filenames (case insensitive)
    #[arg(short, long, default_value = "")]
    filename_filter: String,

    /// Filter for line content (case insensitive)
    #[arg(short = 'L', long, default_value = "")]
    line_filter: String,

    /// Search terms
    #[arg(short, long)]
    search: Vec<String>,

    /// Additional search terms (supports boolean expressions: (term1 & term2) | (term3 & term4))
    #[arg(short, long)]
    additional: Vec<String>,

    /// Number of worker threads to use (defaults to number of CPU cores)
    #[arg(short, long)]
    workers: Option<usize>,
}

#[tokio::main]
async fn main() {
    let mut cli = Cli::parse();
    let mut search_terms = Vec::new();

    // Process search terms
    if cli.search.is_empty() && cli.additional.is_empty() {
        // Default search term if none provided
        add_search_with_expression(&mut search_terms, "", "Master");
    } else {
        // Pad the shorter vector with empty strings
        let max_len = cli.search.len().max(cli.additional.len());
        cli.search.resize(max_len, String::new());
        cli.additional.resize(max_len, String::new());

        // Create search terms from command line arguments
        for i in 0..max_len {
            add_search_with_expression(&mut search_terms, &cli.search[i], &cli.additional[i]);
        }
    }

    // Setup the parser configuration
    let config = ParserConfig {
        log_folder: cli.log_folder,
        output_log: cli.output_log,
        filename_filter: cli.filename_filter,
        line_filter: cli.line_filter,
        search_terms,
        workers: cli.workers,
    };

    // Print header information
    println!("LOG Parser 1.0");
    println!("--------------");
    println!("Filters:");
    println!(" Filename: [{}]", config.filename_filter);
    println!(" Line: [{}]", config.line_filter);
    println!();

    print!("Searching for: ");
    for term in &config.search_terms {
        print!("[{}", term.keyword);
        if let Some(ref expr) = term.additional_expression {
            print!(" + ");
            match expr {
                BooleanExpression::And(terms) => {
                    print!("({})", terms.join(" & "));
                }
                BooleanExpression::Or(sub_exprs) => {
                    let mut first = true;
                    for sub_expr in sub_exprs {
                        if !first {
                            print!(" | ");
                        }
                        first = false;
                        match &**sub_expr {
                            BooleanExpression::And(terms) => {
                                print!("({})", terms.join(" & "));
                            }
                            _ => print!("{:?}", sub_expr), // Simplified for complex expressions
                        }
                    }
                }
            }
        }
        print!("] ");
    }
    println!();
    println!();

    // Configure progress callback
    let progress_callback = |processed: usize, total: usize| {
        let percentage = (processed * 100) / total;
        print!("\rProgress: {}%", percentage);
        stdout().flush().unwrap();
    };

    // Run the parser
    match run_parser(config, Some(progress_callback)).await {
        Ok(result) => {
            println!("\nTotal occurrencies: {}", result.total_matches);
        }
        Err(e) => {
            eprintln!("Error running parser: {}", e);
        }
    }
}