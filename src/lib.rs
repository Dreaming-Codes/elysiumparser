use flate2::read::GzDecoder;
use futures::stream::{self, StreamExt};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tokio::task;

#[derive(Clone, Debug)]
pub struct SearchTerm {
    pub keyword: String,
    pub additional_expression: Option<BooleanExpression>,
}

#[derive(Clone, Debug)]
pub enum BooleanExpression {
    And(Vec<String>),
    Or(Vec<Box<BooleanExpression>>),
}

impl BooleanExpression {
    pub fn parse(expr: &str) -> Option<Self> {
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

    pub fn matches(&self, text: &str) -> bool {
        match self {
            BooleanExpression::And(terms) => terms.iter().all(|term| text.contains(term)),
            BooleanExpression::Or(expressions) => expressions.iter().any(|expr| expr.matches(text)),
        }
    }
}

/// Configuration for the log parser
pub struct ParserConfig {
    pub log_folder: String,
    pub output_log: String,
    pub filename_filter: String,
    pub line_filter: String,
    pub search_terms: Vec<SearchTerm>,
    pub workers: Option<usize>,
}

impl Default for ParserConfig {
    fn default() -> Self {
        Self {
            log_folder: "logs/parser".to_string(),
            output_log: "logs/parser/output.log".to_string(),
            filename_filter: String::new(),
            line_filter: String::new(),
            search_terms: vec![],
            workers: None,
        }
    }
}

/// Result of parsing logs
pub struct ParserResult {
    pub total_matches: usize,
    pub processed_files: usize,
}

/// Add a simple search term
pub fn add_search(search_terms: &mut Vec<SearchTerm>, keyword: &str, additional_keyword: &str) {
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

/// Add a search term with a complex boolean expression
pub fn add_search_with_expression(
    search_terms: &mut Vec<SearchTerm>,
    keyword: &str,
    additional_expr: &str,
) {
    search_terms.push(SearchTerm {
        keyword: keyword.to_lowercase(),
        additional_expression: BooleanExpression::parse(additional_expr),
    });
}

/// Check if a file is a valid log file for processing
pub fn is_valid_log_file(path: &PathBuf, filename_filter: &str, output_log: &str) -> bool {
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

/// Check if a file is a gzipped file
pub fn is_gz_file(path: &PathBuf) -> bool {
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

/// Process a regular log file without progress output
pub fn process_file_silent(
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

/// Process a gzipped log file without progress output
pub fn process_gz_file_silent(
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

/// Process a reader (regular or gzipped file)
pub fn process_reader<R: BufRead>(
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

/// Main parser function that processes all files
pub async fn run_parser(config: ParserConfig, progress_callback: Option<fn(usize, usize)>) -> io::Result<ParserResult> {
    // Convert filters to lowercase
    let filename_filter = config.filename_filter.to_lowercase();
    let line_filter = config.line_filter.to_lowercase();

    // Initialize output file
    if Path::new(&config.output_log).exists() {
        fs::remove_file(&config.output_log)?;
    }

    let log_dir = Path::new(&config.log_folder);
    if !log_dir.exists() {
        fs::create_dir_all(log_dir)?;
    }

    let output_file = Arc::new(Mutex::new(
        OpenOptions::new()
            .write(true)
            .create(true)
            .open(&config.output_log)?,
    ));

    // Collect paths to process
    let mut file_paths = Vec::new();
    match fs::read_dir(&config.log_folder) {
        Ok(entries) => {
            for entry in entries {
                if let Ok(entry) = entry {
                    let path = entry.path();
                    let is_log = is_valid_log_file(&path, &filename_filter, &config.output_log);
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
        Err(e) => return Err(io::Error::new(io::ErrorKind::Other, format!("Error reading log directory: {}", e))),
    }

    // Create shared state
    let search_terms = Arc::new(config.search_terms);
    let line_filter = Arc::new(line_filter);
    let total_match_count = Arc::new(Mutex::new(0));

    // Process files in parallel
    let concurrency = config.workers.unwrap_or_else(num_cpus::get);
    let total_files = file_paths.len();
    let processed_files = Arc::new(Mutex::new(0));
    let progress_mutex = Arc::new(Mutex::new(()));

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
                    // Calculate percentage for the callback
                    let percentage = (*processed * 100) / total_files;
                    let _ = percentage; // Suppress unused variable warning when no callback is provided
                    
                    // Call the progress callback if provided
                    if let Some(callback) = progress_callback {
                        callback(*processed, total_files);
                    }
                }
            })
        })
        .buffer_unordered(concurrency)
        .collect::<Vec<_>>()
        .await;

    let total_matches = *total_match_count.lock().unwrap();
    let processed = *processed_files.lock().unwrap();

    Ok(ParserResult {
        total_matches,
        processed_files: processed,
    })
}