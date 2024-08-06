mod generate;

use std::io::{Read, Seek};
use std::collections::HashMap;

use rayon::prelude::*;

const MEASUREMENTS_FILE: &str = "measurements.txt";

struct Record {
    min: i16,
    max: i16,
    total: i64,
    count: u64,
}

impl Record {
    #[inline]
    fn new(temp: i16) -> Self {
        Self {
            min: temp,
            max: temp,
            total: temp as i64,
            count: 1,
        }
    }

    fn add(&mut self, temp: i16) {
        self.min = self.min.min(temp);
        self.max = self.max.max(temp);
        self.total += temp as i64;
        self.count += 1;
    }

    fn mean(&self) -> f64 {
        (self.total as f64 / self.count as f64) / 10.
    }

    fn min(&self) -> f64 {
        self.min as f64 / 10.
    }

    fn max(&self) -> f64 {
        self.max as f64 / 10.
    }

    #[inline]
    fn combine(&mut self, other: &Self) {
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
        self.total += other.total;
        self.count += other.count;
    }
}

fn main() {
    let start_time = std::time::Instant::now();
    // This file reads in 1 billion rows of data from
    // MEASUREMENTS_FILE.
    // The file is a "csv" file, each line being name;temp
    // name is a string and temp is a float with one decimal

    // Stream in the file as bytes, not all at once
    let file = std::fs::File::open(MEASUREMENTS_FILE);

    // If the file is not found, generate the file
    if file.is_err() {
        println!("File not found, generating file");
        let _ = generate::main();
        return;
    }

    let file = file.unwrap();

    let size = file.metadata().unwrap().len();

    // Read in the file in chunks
    let mut chunk_start = 0;
    let mut chunk_end = 0;
    // Chunk size is file size divided by number of threads
    let chunk_size = size / rayon::current_num_threads() as u64;

    println!("Chunk size: {}", chunk_size);

    // Create a vector of tuples, each tuple containing the start and end of a chunk
    let chunk_specs = (0..rayon::current_num_threads()).map(|_| {
        chunk_end = chunk_start + chunk_size;

        // The end of the chunk doesn't necessarily end at the end of a line, 
        // so we need to read until we hit a \n character
        // We do this by creating a new reader for each chunk, seeking to the end of the chunk,
        // and reading until we hit a \n character
        let mut reader = std::io::BufReader::new(std::fs::File::open(MEASUREMENTS_FILE).unwrap());
        reader.seek(std::io::SeekFrom::Start(chunk_end)).unwrap();
        let mut reader_bytes = reader.bytes();
        let mut offset = 0;

        while let Some(Ok(c)) = reader_bytes.next() {
            offset += 1;
            if c == b'\n' {
                break;
            }
        }

        let chunk_end = chunk_end + offset;

        // Return the start and end of the chunk
        let to_return = (chunk_start, size.min(chunk_end));

        // Before next loop, set the start of the next chunk to the end of the current chunk
        chunk_start = chunk_end;
        
        to_return
    }).collect::<Vec<(u64, u64)>>();

    println!("{:?}", chunk_specs);

    // Parallelize the reading of the file, calling the read_chunk function on each chunk
    let data = chunk_specs.into_par_iter().map(|(start, end)| {
        read_chunk(MEASUREMENTS_FILE, start, end)
    }).reduce(HashMap::new, |mut map1, map2| {
        for (key, value) in map2 {
            if map1.contains_key(&key) {
                map1.get_mut(&key).unwrap().combine(&value);
            } else {
                map1.insert(key.clone(), value);
            }
        }

        map1
    });


    /*
    The program should print out the min, mean, and max values per station, alphabetically ordered. The format that is expected varies slightly from language to language, but the following example shows the expected output for the first three stations:

    Hamburg;12.0;23.1;34.2
    Bulawayo;8.9;22.1;35.2
    Palembang;38.8;39.9;41.0
     */

    let mut data = data.into_iter().collect::<Vec<_>>();
    data.sort_unstable_by(|a, b| a.0.cmp(&b.0));

    for (key, value) in data {
        let min = (value.min() * 10.).round() / 10.;
        let mean = (value.mean() * 10.).round() / 10.;
        let max = (value.max() * 10.).round() / 10.;
        println!("{};{};{};{}", std::str::from_utf8(&key).unwrap(), min, mean, max);
    }


    println!("Time taken: {:?}", start_time.elapsed());
}

fn read_chunk(file: &str, start: u64, end: u64) -> HashMap<Vec<u8>, Record>{
    let mut reader = std::io::BufReader::new(std::fs::File::open(file).unwrap());
    reader.seek(std::io::SeekFrom::Start(start)).unwrap();
    let mut reader_bytes = reader.bytes();

    // Return a hashmap of the data, with the name as the key and the values of
    // - min
    // - max
    // - total
    // - count
    // All temps are multiplied by 10
    
    // Quickest hasher in std
    let mut data_map: HashMap<Vec<u8>, Record> = std::collections::HashMap::with_capacity_and_hasher(10_000, Default::default());

    let mut bytes_consumed = 0;
    let mut c;
    
    let mut name = Vec::with_capacity(124);
    let mut temp = Vec::with_capacity(8);

    let total_bytes = end - start;

    loop {
        name.clear();
        temp.clear();

        // Read in the name, byte by byte until
        // the semicolon is found. We don't want
        // to include the semicolon in the name so
        // we break the loop when we find it
        loop {
            c = reader_bytes.next().unwrap().unwrap();
            bytes_consumed += 1;

            if c == b';' {
                break;
            } else {
                name.push(c);
            }
        }

        // Read in the temperature, byte by byte.
        // It's the same general idea as the name,
        // but we also need to check for a period
        // which we skip
        loop {
            c = reader_bytes.next().unwrap().unwrap();
            bytes_consumed += 1;

            if c == b'\n' {
                break;
            } else if c != b'.' {
                temp.push(c);
            }
        }
        
        // Read from the cache if the temperature has been seen before
        // Otherwise, parse the temperature and add it to the cache
        let temp_num: i16 = atoi_simd::parse(&temp).unwrap();

        if let Some(entry) = data_map.get_mut(&name) {
            entry.add(temp_num as i16);
        } else {
            data_map.insert(name.clone(), Record::new(temp_num as i16));
        }

        if bytes_consumed >= total_bytes {
            break;
        }
    }

    println!("Bytes consumed: {}", bytes_consumed);

    data_map
}