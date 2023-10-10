use futures::executor::block_on;
use std::fs::File;
use std::io::{BufRead, BufReader};
use tantivy::schema::{NumericOptions, Schema, TEXT};
use tantivy::{doc, IndexBuilder, IndexSettings, IndexSortByField, Order, Term};
use tantivy::tokenizer::{RemoveLongFilter, TextAnalyzer, TokenizerManager};
use whitespace_tokenizer_fork::WhitespaceTokenizer;
use std::path::PathBuf;

const INPUT_FILE_PATH: &str = "/Volumes/workplace/Tantivy-JNI-Prototype/mylib/src/bin/input_data.txt";

pub fn build_index(output_dir: &PathBuf, index_delete_pct: i32) -> tantivy::Result<()> {
    println!("Build index at `{}` with delete_pct {}%", output_dir.display(), index_delete_pct);

    let mut schema_builder = Schema::builder();

    let body = schema_builder.add_text_field(
        "body",
        TEXT.set_indexing_options(
            TEXT.get_indexing_options()
                .unwrap()
                .clone()
                .set_tokenizer("whitespace"),
        ),
    );
    let id_field = schema_builder.add_u64_field(
        "id",
        NumericOptions::default()
            .set_indexed()
            .set_fast(),
    );
    let schema = schema_builder.build();

    let index = IndexBuilder::new()
        .schema(schema)
        .tokenizers(get_tokenizer_manager())
        .settings(IndexSettings {
            sort_by_field: Some(IndexSortByField {
                order: Order::Asc,
                field: "id".into(),
            }),
	    docstore_compress_dedicated_thread: false,
            ..IndexSettings::default()
        })
        .create_in_dir(output_dir)
        .expect("Failed to create index");

    let mut i = 0;
    let mut num_skipped = 0;
    {
        let mut index_writer = index
            .writer_with_num_threads(4, 2_000_000_000)
            .expect("failed to create index writer");
        let input_file = File::open(PathBuf::from(INPUT_FILE_PATH))?;
        let reader = BufReader::new(input_file);


        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            // (title, date, body, label)
            let parsed_line: Vec<&str> = line.split('\t').collect();
            if parsed_line.len() != 4 {
                println!("Skipping malformed line: {}", line);
                num_skipped += 1;
                continue;
            }
            i += 1;
            if i % 100_000 == 0 {
                println!("{}", i);
            }
            let doc = doc!(
                id_field => i as u64,
                body => parsed_line[2]
            );
            index_writer.add_document(doc).unwrap();
        }

        index_writer.commit()?;
        index_writer.wait_merging_threads()?;
    }
    let segment_ids = index.searchable_segment_ids()?;
    let mut index_writer = index
        .writer(1_500_000_000)
        .expect("failed to create index writer");
    block_on(index_writer.merge(&segment_ids))?;

    // Apply deletes
    let total_indexed = i;
    let mut num_deleted = 0;
    for i in 1..=total_indexed {
        if i % 100 < index_delete_pct {
            index_writer.delete_term(Term::from_field_u64(id_field, i as u64));
            num_deleted += 1;
        }
    }
    index_writer.commit()?;

    block_on(index_writer.garbage_collect_files())?;
    println!("Done. Read {i} docs, skipped {num_skipped}, deleted {num_deleted}");
    Ok(())
}


pub fn get_tokenizer_manager() -> TokenizerManager {
    let tokenzier_manager = TokenizerManager::default();
    let tokenizer = TextAnalyzer::builder(WhitespaceTokenizer).filter(RemoveLongFilter::limit(256)).build();
    tokenzier_manager.register("whitespace", tokenizer);
    tokenzier_manager
}

mod whitespace_tokenizer_fork {

    use tantivy::tokenizer::{Token, Tokenizer, TokenStream};
    use std::str::CharIndices;

    /// Tokenize the text by splitting on whitespaces.
    #[derive(Clone)]
    pub struct WhitespaceTokenizer;

    pub struct WhitespaceTokenStream<'a> {
        text: &'a str,
        chars: CharIndices<'a>,
        token: Token,
    }

    impl Tokenizer for WhitespaceTokenizer {
        type TokenStream<'a> = WhitespaceTokenStream<'a>;

        fn token_stream<'a>(&'a mut self, text: &'a str) -> WhitespaceTokenStream<'a> {
            WhitespaceTokenStream {
                text,
                chars: text.char_indices(),
                token: Token::default(),
            }
        }
    }

    impl<'a> WhitespaceTokenStream<'a> {
        // search for the end of the current token.
        fn search_token_end(&mut self) -> usize {
            (&mut self.chars)
                .filter(|&(_, ref c)| c.is_whitespace())
                .map(|(offset, _)| offset)
                .next()
                .unwrap_or(self.text.len())
        }
    }

    impl<'a> TokenStream for WhitespaceTokenStream<'a> {
        fn advance(&mut self) -> bool {
            self.token.text.clear();
            self.token.position = self.token.position.wrapping_add(1);
            while let Some((offset_from, c)) = self.chars.next() {
                if !c.is_whitespace() {
                    let offset_to = self.search_token_end();
                    self.token.offset_from = offset_from;
                    self.token.offset_to = offset_to;
                    self.token.text.push_str(&self.text[offset_from..offset_to]);
                    return true;
                }
            }
            false
        }

        fn token(&self) -> &Token {
            &self.token
        }

        fn token_mut(&mut self) -> &mut Token {
            &mut self.token
        }
    }

    #[cfg(test)]
    mod tests {
        use tantivy::tokenizer::{TextAnalyzer, Token};

        use super::WhitespaceTokenizer;

        #[test]
        fn test_whitespace_tokenizer_with_unicode_spaces() {
            let tokens = token_stream_helper("わ |　か　　花");
            assert_eq!(tokens.len(), 4);
        }

        fn token_stream_helper(text: &str) -> Vec<Token> {
            let mut a = TextAnalyzer::from(WhitespaceTokenizer);
            let mut token_stream = a.token_stream(text);
            let mut tokens: Vec<Token> = vec![];
            let mut add_token = |token: &Token| {
                tokens.push(token.clone());
            };
            token_stream.process(&mut add_token);
            tokens
        }
    }
}

