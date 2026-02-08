#include <zstd.h>
#include <fstream>
#include <iostream>
#include <unordered_map>
#include <vector>
#include <string>
#include <sstream>
#include <chrono>
#include <iomanip>
#include <filesystem>


using namespace std;

// static const long TOTAL_GAMES = 94847276;
static const long TOTAL_GAMES = 1000000;
// static const int TOTAL_CSV = 100000;
static const int BATCH_SIZE = 2000;
static const string CSV_FILE = "C:/Users/gagah/Documents/Portofolios/Chess-Analysis/chess-analysis/from_cpp_lichess_rapid_elo2000.csv";

/* ============================
   PGN HEADER PARSER
   ============================ */
unordered_map<string, string> parse_headers(const vector<string>& lines) {
    unordered_map<string, string> headers;

    for (const auto& line : lines) {
        if (line.empty() || line[0] != '[') continue;

        size_t key_start = 1;
        size_t key_end = line.find(' ');
        size_t val_start = line.find('"') + 1;
        size_t val_end = line.rfind('"');

        if (key_end == string::npos || val_start == string::npos || val_end == string::npos)
            continue;

        string key = line.substr(key_start, key_end - key_start);
        string val = line.substr(val_start, val_end - val_start);
        headers[key] = val;
    }
    return headers;
}

/* ============================
   PROCESS GAME (FILTER LOGIC)
   ============================ */
bool process_game(
    const unordered_map<string, string>& h,
    string& csv_row
) {
    try {
        int white_elo = stoi(h.at("WhiteElo"));
        int black_elo = stoi(h.at("BlackElo"));

        if (white_elo < 2000 || black_elo < 2000)
            return false;

        string tc = h.at("TimeControl");
        int base = stoi(tc.substr(0, tc.find('+')));
        if (base < 600 || base > 1800)
            return false;

        // Build CSV row
        csv_row =
            h.at("Site") + "," +
            h.at("White") + "," +
            h.at("Black") + "," +
            h.at("WhiteTitle") + "," +
            h.at("BlackTitle") + "," +
            to_string(white_elo) + "," +
            to_string(black_elo) + "," +
            h.at("Result") + "," +
            h.at("WhiteRatingDiff") + "," +
            h.at("BlackRatingDiff") + "," +
            tc + "," +
            h.at("Termination") + "," +
            h.at("ECO") + "," +
            h.at("Opening");

        return true;

    } catch (...) {
        return false;
    }
}

// ============================
// PROGRESS LOGGER (tqdm-style)
// ============================
void log_progress(
    long scanned,
    long collected,
    long total,
    std::chrono::steady_clock::time_point start_time
) {
    using namespace std::chrono;

    auto now = steady_clock::now();
    double elapsed_sec = duration_cast<seconds>(now - start_time).count();

    double speed = scanned > 0 ? scanned / elapsed_sec : 0.0;
    double pct = (double)scanned / total * 100.0;
    double eta_sec = speed > 0 ? (total - scanned) / speed : 0.0;

    cerr << "\r[INFO] "
         << "Scanned: " << scanned
         << " | Collected: " << collected
         << " | " << fixed << setprecision(2) << pct << "%"
         << " | Speed: " << fixed << setprecision(0) << speed << " g/s"
         << " | Elapsed: " << (long)elapsed_sec << " s"
         << " | ETA: " << (long)eta_sec << " s"
         << flush;
}


/* ============================
   MAIN
   ============================ */
int main() {
    ifstream file("C:/Users/gagah/Documents/Portofolios/Chess-Analysis/lichess_db_standard_rated_2025-12.pgn.zst", ios::binary);
    if (!file) {
        cerr << "Cannot open file\n";
        return 1;
    }

    cerr << "[INFO] Starting PGN parsing..." << endl;
    auto start_time = std::chrono::steady_clock::now();

    ofstream csv(CSV_FILE, ios::app);
    if (csv.tellp() == 0) {
        csv << "link,white,black,white_titled,black_titled,white_elo,black_elo,"
               "result,white_rating_diff,black_rating_diff,time_control,"
               "termination,eco,opening\n";
    }
    cerr << "[INFO] CSV output path: "
        << std::filesystem::absolute(CSV_FILE)
        << endl;


    ZSTD_DStream* dstream = ZSTD_createDStream();
    ZSTD_initDStream(dstream);

    const size_t BUFSIZE = ZSTD_DStreamOutSize();
    vector<char> inbuf(ZSTD_DStreamInSize());
    vector<char> outbuf(BUFSIZE);

    vector<string> game_lines;
    int collected = 0;
    int batch_count = 0;
    long scanned_games = 0;

    while (file && scanned_games < TOTAL_GAMES) {  //  && collected < TOTAL_CSV
        file.read(inbuf.data(), inbuf.size());
        ZSTD_inBuffer input{inbuf.data(), (size_t)file.gcount(), 0};

        while (input.pos < input.size) {
            ZSTD_outBuffer output{outbuf.data(), outbuf.size(), 0};
            ZSTD_decompressStream(dstream, &output, &input);

            string chunk(outbuf.data(), output.pos);
            stringstream ss(chunk);
            string line;

            while (getline(ss, line)) {
                if (line.empty() && !game_lines.empty()) {

                // ===== game parsed =====
                scanned_games++;

                auto headers = parse_headers(game_lines);
                string row;

                if (process_game(headers, row)) {
                    csv << row << "\n";
                    collected++;
                }

                // ===== tqdm-style logging =====
                if (scanned_games % 100000 == 0) {
                    csv.flush();
                    log_progress(scanned_games, collected, TOTAL_GAMES, start_time);
                }

                game_lines.clear();
                } else {
                    game_lines.push_back(line);
                }
            }

            // if (collected >= TOTAL_CSV) {
            //     cerr << "\n[INFO] Target CSV rows reached: "
            //         << collected << endl;
            //     break;
            // }
        }
    }

    ZSTD_freeDStream(dstream);
    csv.close();

    auto end_time = std::chrono::steady_clock::now();
    auto total_sec =
        std::chrono::duration_cast<std::chrono::seconds>(end_time - start_time).count();

    cerr << "\n[INFO] Finished." << endl;
    cerr << "[INFO] Total scanned   : " << scanned_games << endl;
    cerr << "[INFO] Total collected : " << collected << endl;
    cerr << "[INFO] Total time      : " << total_sec << " seconds" << endl;

    return 0;
}
