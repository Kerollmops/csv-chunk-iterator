use std::env;
use slice_group_by::StrGroupBy;

fn main() {
    let filepath = env::args().nth(1).unwrap();

    let mut rdr = csv::Reader::from_path(&filepath).unwrap();
    let mut number_of_words = 0;

    for result in rdr.records() {
        let record = result.unwrap();

        let count: usize = record.iter()
            // We count the number of words in each field.
            .map(|s| {
                // We create two groups: spaces and non-spaces.
                s.linear_group_by_key(|c| c.is_whitespace())
                    // We ignore the groups composed of spaces.
                    .filter(|s| !s.chars().nth(0).map_or(true, |c| c.is_whitespace()))
                    // We now count the number of groups that are not whitespaces.
                    .count()
            })
            // We now sums up the counts of words.
            .sum();

        number_of_words += count;
    }

    println!("We found {} words.", number_of_words);
}
