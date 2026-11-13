//! jobber-host's banner: its name in block letters beside jobber's mark, then
//! jobber's tagline, as `--help`, `--version` and the README open.

/// jobber's letters, then `-host`, in the same face.
const LETTERS: [&str; 6] = [
    "     ██╗ ██████╗ ██████╗ ██████╗ ███████╗██████╗       ██╗  ██╗ ██████╗ ███████╗████████╗",
    "     ██║██╔═══██╗██╔══██╗██╔══██╗██╔════╝██╔══██╗      ██║  ██║██╔═══██╗██╔════╝╚══██╔══╝",
    "     ██║██║   ██║██████╔╝██████╔╝█████╗  ██████╔╝█████╗███████║██║   ██║███████╗   ██║",
    "██   ██║██║   ██║██╔══██╗██╔══██╗██╔══╝  ██╔══██╗╚════╝██╔══██║██║   ██║╚════██║   ██║",
    "╚█████╔╝╚██████╔╝██████╔╝██████╔╝███████╗██║  ██║      ██║  ██║╚██████╔╝███████║   ██║",
    " ╚════╝  ╚═════╝ ╚═════╝ ╚═════╝ ╚══════╝╚═╝  ╚═╝      ╚═╝  ╚═╝ ╚═════╝ ╚══════╝   ╚═╝",
];

/// jobber's mark (`MARK_PIXELS` in jobber's `src/banner.ts`), on its 18 by 12
/// grid: three shipping containers, ribbed, one stacked on two, the runs each
/// going out in its own image. The first and last rows are blank so a
/// terminal, drawing two rows a character, gives it as many rows as the letters.
const MARK_PIXELS: [&str; 12] = [
    "..................",
    "....##########....",
    "....#.#.##.#.#....",
    "....#.#.##.#.#....",
    "....##########....",
    "..................",
    "########..########",
    "#.#..#.#..#.#..#.#",
    "#.#..#.#..#.#..#.#",
    "#.#..#.#..#.#..#.#",
    "########..########",
    "..................",
];

/// jobber's line, verbatim.
pub const TAGLINE: &str = "Submit code to run on whatever computers you have.";

/// What jobber-host is, in one line.
pub const SUMMARY: &str =
    "jobber-host runs jobber's runs on this computer, each in its repo's podman image.";

/// The mark as text, two pixels a character.
fn mark() -> Vec<String> {
    MARK_PIXELS
        .chunks(2)
        .map(|rows| {
            rows[0]
                .chars()
                .zip(rows[1].chars())
                .map(|(top, bottom)| match (top == '#', bottom == '#') {
                    (true, true) => '█',
                    (true, false) => '▀',
                    (false, true) => '▄',
                    (false, false) => ' ',
                })
                .collect::<String>()
                .trim_end()
                .to_string()
        })
        .collect()
}

/// The letters with the mark two columns to their right.
pub fn wordmark() -> String {
    let width = LETTERS.iter().map(|l| l.chars().count()).max().unwrap_or(0);
    LETTERS
        .iter()
        .zip(mark())
        .map(|(line, mark)| {
            let pad = width - line.chars().count();
            format!("{line}{}  {mark}", " ".repeat(pad))
                .trim_end()
                .to_string()
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Painted for a terminal that wants colour (https://no-color.org): jobber's violet.
pub struct Palette(bool);

impl Palette {
    /// Colour only when stdout is a terminal and NO_COLOR is unset or empty.
    pub fn for_stdout() -> Palette {
        use std::io::IsTerminal;
        Palette::new(
            std::io::stdout().is_terminal(),
            std::env::var_os("NO_COLOR")
                .filter(|v| !v.is_empty())
                .is_some(),
        )
    }

    pub fn new(terminal: bool, no_color: bool) -> Palette {
        Palette(terminal && !no_color)
    }

    fn paint(&self, code: &str, text: &str) -> String {
        if self.0 {
            format!("\x1b[{code}m{text}\x1b[0m")
        } else {
            text.to_string()
        }
    }

    pub fn mark(&self, text: &str) -> String {
        self.paint("38;5;98", text)
    }

    pub fn bold(&self, text: &str) -> String {
        self.paint("1", text)
    }

    pub fn dim(&self, text: &str) -> String {
        self.paint("2", text)
    }
}

/// The block letters beside the mark, then the tagline, as `--help` and `--version` open.
pub fn banner(paint: &Palette) -> String {
    format!(
        "\n{}\n\n{}\n{SUMMARY}\n",
        paint.mark(&wordmark()),
        paint.bold(TAGLINE)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_mark_stands_beside_the_letters_on_every_row() {
        let wordmark = wordmark();
        let rows: Vec<&str> = wordmark.lines().collect();
        assert_eq!(rows.len(), 6);
        for (row, (letters, mark)) in rows.iter().zip(LETTERS.iter().zip(mark())) {
            assert!(row.starts_with(letters.trim_end()), "{row}");
            assert!(row.ends_with(&mark), "{row}");
        }
    }

    #[test]
    fn the_mark_is_jobbers_as_its_terminal_draws_it() {
        assert_eq!(
            mark(),
            [
                "    ▄▄▄▄▄▄▄▄▄▄",
                "    █ █ ██ █ █",
                "    ▀▀▀▀▀▀▀▀▀▀",
                "█▀█▀▀█▀█  █▀█▀▀█▀█",
                "█ █  █ █  █ █  █ █",
                "▀▀▀▀▀▀▀▀  ▀▀▀▀▀▀▀▀",
            ]
            .map(String::from)
        );
    }

    #[test]
    fn it_is_violet_only_on_a_terminal_without_no_color() {
        let colour = banner(&Palette::new(true, false));
        assert!(colour.contains("\x1b[38;5;98m"), "{colour}");
        for plain in [Palette::new(false, false), Palette::new(true, true)] {
            let text = banner(&plain);
            assert!(!text.contains('\x1b'), "{text}");
            assert!(text.contains(&wordmark()) && text.contains(TAGLINE));
        }
    }

    #[test]
    fn the_readme_opens_with_the_banner_and_the_tagline_verbatim() {
        let readme = include_str!("../README.md");
        let opening = format!(
            "# jobber-host\n\n```\n{}\n```\n\n**{TAGLINE}**\n",
            wordmark()
        );
        assert!(
            readme.starts_with(&opening),
            "README.md must open with:\n{opening}"
        );
    }
}
