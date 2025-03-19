use clap::Parser;
use flate2::read::GzDecoder;
use futures::stream::{self, StreamExt};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write, stdout};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tokio::task;

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

#[derive(Clone)]
struct SearchTerm {
    keyword: String,
    additional_expression: Option<BooleanExpression>,
}

#[derive(Clone, Debug)]
enum BooleanExpression {
    And(Vec<String>),
    Or(Vec<Box<BooleanExpression>>),
}

impl BooleanExpression {
    fn parse(expr: &str) -> Option<Self> {
        if expr.is_empty() {
            return None;
        }

        // Check if the expression has OR operators at the top level
        if expr.contains("|") {
            let or_parts: Vec<&str> = expr.split("|").map(|s| s.trim()).collect();
            let or_expressions: Vec<Box<BooleanExpression>> = or_parts
                .iter()
                .filter_map(|part| {
                    // Remove surrounding parentheses if present
                    let clean_part = part.trim_start_matches('(').trim_end_matches(')').trim();
                    BooleanExpression::parse(clean_part).map(Box::new)
                })
                .collect();

            if !or_expressions.is_empty() {
                return Some(BooleanExpression::Or(or_expressions));
            }
        }

        // If no OR operator or only one part, treat as AND expression
        let clean_expr = expr.trim_start_matches('(').trim_end_matches(')').trim();

        // Check if it has explicit AND operators
        if clean_expr.contains(" & ") {
            let and_parts: Vec<String> = clean_expr
                .split(" & ")
                .map(|s| s.trim().to_lowercase())
                .collect();
            return Some(BooleanExpression::And(and_parts));
        }

        // Single term
        Some(BooleanExpression::And(vec![clean_expr.to_lowercase()]))
    }

    fn matches(&self, text: &str) -> bool {
        match self {
            BooleanExpression::And(terms) => terms.iter().all(|term| text.contains(term)),
            BooleanExpression::Or(expressions) => expressions.iter().any(|expr| expr.matches(text)),
        }
    }
}

#[tokio::main]
async fn main() {
    let mut cli = Cli::parse();
    let mut search_terms = Vec::new();

    // Process search terms
    if cli.search.is_empty() && cli.additional.is_empty() {
        // Default search term if none provided
        add_search(&mut search_terms, "", "Master");
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

    // Convert filters to lowercase
    let filename_filter = cli.filename_filter.to_lowercase();
    let line_filter = cli.line_filter.to_lowercase();

    // Initialize and print header
    println!("Filters:");
    println!(" Filename: [{}]", filename_filter);
    println!(" Line: [{}]", line_filter);
    println!();

    print!("Searching for: ");
    for term in &search_terms {
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

    // Initialize output file
    if Path::new(&cli.output_log).exists() {
        fs::remove_file(&cli.output_log).expect("Failed to delete existing output log");
    }

    let log_dir = Path::new(&cli.log_folder);
    if !log_dir.exists() {
        fs::create_dir_all(log_dir).expect("Failed to create log directory");
    }

    let output_file = Arc::new(Mutex::new(
        OpenOptions::new()
            .write(true)
            .create(true)
            .open(&cli.output_log)
            .expect("Failed to create output file"),
    ));

    // Collect paths to process
    let mut file_paths = Vec::new();
    match fs::read_dir(&cli.log_folder) {
        Ok(entries) => {
            for entry in entries {
                if let Ok(entry) = entry {
                    let path = entry.path();
                    let is_log = is_valid_log_file(&path, &filename_filter, &cli.output_log);
                    let is_gz = is_gz_file(&path)
                        && path
                            .to_string_lossy()
                            .to_lowercase()
                            .contains(&filename_filter);

                    if is_log || is_gz {
                        file_paths.push(path);
                    }
                }
            }
        }
        Err(e) => eprintln!("Error reading log directory: {}", e),
    }

    // Create shared state
    let search_terms = Arc::new(search_terms);
    let line_filter = Arc::new(line_filter);
    let total_match_count = Arc::new(Mutex::new(0));

    // Process files in parallel
    let concurrency = cli.workers.unwrap_or_else(num_cpus::get);
    let total_files = file_paths.len();
    let processed_files = Arc::new(Mutex::new(0));
    let progress_mutex = Arc::new(Mutex::new(()));

    println!(
        "Using {} worker threads to process {} files",
        concurrency, total_files
    );
    print!("Progress: 0%");
    stdout().flush().unwrap();

    stream::iter(file_paths)
        .map(|path| {
            let search_terms = Arc::clone(&search_terms);
            let line_filter = Arc::clone(&line_filter);
            let output_file = Arc::clone(&output_file);
            let total_match_count = Arc::clone(&total_match_count);
            let processed_files = Arc::clone(&processed_files);
            let progress_mutex = Arc::clone(&progress_mutex);

            task::spawn(async move {
                let is_gz = is_gz_file(&path);
                let file_match_count = if is_gz {
                    match process_gz_file_silent(&path, &search_terms, &line_filter, &output_file) {
                        Ok(count) => count,
                        Err(e) => {
                            eprintln!("Error processing gzip file {}: {}", path.display(), e);
                            0
                        }
                    }
                } else {
                    process_file_silent(&path, &search_terms, &line_filter, &output_file)
                };

                // Update total count
                {
                    let mut count = total_match_count.lock().unwrap();
                    *count += file_match_count;
                }

                // Update progress
                {
                    let _lock = progress_mutex.lock().unwrap();
                    let mut processed = processed_files.lock().unwrap();
                    *processed += 1;
                    let percentage = (*processed * 100) / total_files;
                    // Use \r to return to beginning of line and overwrite previous progress
                    print!("\rProgress: {}%", percentage);
                    // Ensure output is displayed immediately
                    std::io::stdout().flush().unwrap();
                }
            })
        })
        .buffer_unordered(concurrency)
        .collect::<Vec<_>>()
        .await;

    // Print summary
    println!(
        "\nTotal occurrencies: {}",
        *total_match_count.lock().unwrap()
    );
}

fn add_search(search_terms: &mut Vec<SearchTerm>, keyword: &str, additional_keyword: &str) {
    search_terms.push(SearchTerm {
        keyword: keyword.to_lowercase(),
        additional_expression: if additional_keyword.is_empty() {
            None
        } else {
            Some(BooleanExpression::And(vec![
                additional_keyword.to_lowercase(),
            ]))
        },
    });
}

fn add_search_with_expression(
    search_terms: &mut Vec<SearchTerm>,
    keyword: &str,
    additional_expr: &str,
) {
    search_terms.push(SearchTerm {
        keyword: keyword.to_lowercase(),
        additional_expression: BooleanExpression::parse(additional_expr),
    });
}

fn is_valid_log_file(path: &PathBuf, filename_filter: &str, output_log: &str) -> bool {
    if !path.is_file() {
        return false;
    }

    if let Some(extension) = path.extension() {
        if extension != "log" {
            return false;
        }
    } else {
        return false;
    }

    let output_path = Path::new(output_log);
    if path == output_path {
        return false;
    }

    if let Some(filename) = path.file_name() {
        if let Some(filename_str) = filename.to_str() {
            // Skip files starting with "debug"
            if filename_str.to_lowercase().starts_with("debug") {
                return false;
            }

            return filename_str.to_lowercase().contains(filename_filter);
        }
    }

    false
}

fn is_gz_file(path: &PathBuf) -> bool {
    if !path.is_file() {
        return false;
    }

    if let Some(extension) = path.extension() {
        if extension != "gz" {
            return false;
        }
    } else {
        return false;
    }

    // Skip files starting with "debug"
    if let Some(filename) = path.file_name() {
        if let Some(filename_str) = filename.to_str() {
            if filename_str.to_lowercase().starts_with("debug") {
                return false;
            }
            return true;
        }
    }

    false
}

// Silent version (no progress output for individual files)
fn process_file_silent(
    path: &PathBuf,
    search_terms: &[SearchTerm],
    line_filter: &str,
    output_file: &Arc<Mutex<File>>,
) -> usize {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(e) => {
            eprintln!("Error opening file {}: {}", path.display(), e);
            return 0;
        }
    };

    let reader = BufReader::new(file);
    process_reader(reader, search_terms, line_filter, output_file)
}

// Silent version (no progress output for individual files)
fn process_gz_file_silent(
    gz_path: &PathBuf,
    search_terms: &[SearchTerm],
    line_filter: &str,
    output_file: &Arc<Mutex<File>>,
) -> Result<usize, io::Error> {
    let file = File::open(gz_path)?;
    let gz = GzDecoder::new(file);
    let reader = BufReader::new(gz);
    Ok(process_reader(
        reader,
        search_terms,
        line_filter,
        output_file,
    ))
}

fn process_reader<R: BufRead>(
    reader: R,
    search_terms: &[SearchTerm],
    line_filter: &str,
    output_file: &Arc<Mutex<File>>,
) -> usize {
    let mut file_match_count = 0;

    for line in reader.lines() {
        if let Ok(line) = line {
            let lowercase_line = line.to_lowercase();

            let is_match = search_terms.iter().any(|term| {
                // Check if line contains the primary filter
                if !lowercase_line.contains(line_filter) {
                    return false;
                }

                // Check if line contains the main keyword (if not empty)
                if !term.keyword.is_empty() && !lowercase_line.contains(&term.keyword) {
                    return false;
                }

                // Check if line satisfies the additional expression (if any)
                match &term.additional_expression {
                    Some(expr) => expr.matches(&lowercase_line),
                    None => true,
                }
            });

            if is_match {
                file_match_count += 1;

                // Write to the output file with mutex lock
                if let Ok(mut file) = output_file.lock() {
                    if let Err(e) = writeln!(file, "{}", line) {
                        eprintln!("Error writing to output file: {}", e);
                    }
                }
            }
        }
    }

    file_match_count
}
