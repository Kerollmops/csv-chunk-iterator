use std::fs::File;
use std::env;

struct IterCsvChunks<'a> {
    chunk_size: usize,
    bytes: &'a [u8],
    headers: csv::ByteRecord,
    rdr: csv::Reader<&'a [u8]>,
    last_position: csv::Position,
}

impl<'a> IterCsvChunks<'a> {
    fn new(bytes: &'a [u8], chunk_size: usize) -> csv::Result<IterCsvChunks<'a>> {
        let mut rdr = csv::Reader::from_reader(bytes);
        let headers = rdr.byte_headers()?.clone();
        let last_position = rdr.position().clone();
        Ok(IterCsvChunks { chunk_size, bytes, headers, rdr, last_position })
    }
}

impl<'a> Iterator for IterCsvChunks<'a> {
    type Item = csv::Result<csv::Reader<&'a [u8]>>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut record = csv::ByteRecord::new();
        let mut count = 0;
        let mut position;

        loop {
            position = self.rdr.position().clone();
            match self.rdr.read_byte_record(&mut record) {
                Ok(true) => (),
                Ok(false) => break,
                Err(e) => return Some(Err(e)),
            };

            count += 1;

            if count == self.chunk_size {
                let start = self.last_position.byte() as usize;
                let end = position.byte() as usize;
                let slice = &self.bytes[start..end];

                self.last_position = position;

                // We create a new reader starting at the given number of records.
                let mut new_rdr = csv::Reader::from_reader(slice);
                new_rdr.set_byte_headers(self.headers.clone());

                return Some(Ok(new_rdr));
            }
        }

        if count == 0 {
            None
        } else {
            let start = self.last_position.byte() as usize;
            let end = position.byte() as usize;
            let slice = &self.bytes[start..end];

            let mut new_rdr = csv::Reader::from_reader(slice);
            new_rdr.set_byte_headers(self.headers.clone());

            Some(Ok(new_rdr))
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let filepath = env::args().nth(1).unwrap();
    let file = File::open(filepath)?;
    let csv = unsafe { memmap::Mmap::map(&file)? };

    let iter = IterCsvChunks::new(&csv, 10000)?;

    let mut number_of_records = 0;
    let mut record = csv::ByteRecord::new();

    for result in iter {
        let mut chunk_rdr = result?;
        while chunk_rdr.read_byte_record(&mut record)? {
            number_of_records += 1;
        }
    }

    println!("{}", number_of_records);

    Ok(())
}
