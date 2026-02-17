#include <zstd.h>
#include <fstream>
#include <iostream>
#include <vector>
#include <string>
#include <sstream>
#include <unordered_map>
#include <regex>
#include <chrono>
#include <iomanip>
#include <filesystem>

#ifdef _WIN32
#include <windows.h>
#endif


using namespace std;
using namespace std::chrono;
namespace fs = std::filesystem;

static const int games_to_read = 100000;  // jumlah game yang ingin dilihat
static const size_t  TOTAL_GAMES = 94847276;
static const size_t  BATCH_SIZE = 5000;
static const size_t  LOG_CHECKPOINT = 20000;

constexpr int MIN_ELO = 2200;

fs::path FILE_NAME = "cpp_lichess_blitz_elo" + std::to_string(MIN_ELO) + "_100k.csv";
fs::path BASE_PATH = "C:/Users/gagah/Documents/Portofolios/Chess-analysis";
fs::path SOURCE_PATH = BASE_PATH / "lichess_db_standard_rated_2025-12.pgn.zst";
fs::path OUTPUT_PATH = BASE_PATH / "data" / FILE_NAME;

using Schema = std::vector<std::pair<std::string, std::string>>;
const Schema CSV_SCHEMA = {
    {"Event", "event"},
    {"Site", "link"},
    {"Date", "date"},
    {"White", "white"},
    {"Black", "black"},
    {"WhiteTitle", "white_titled"},
    {"BlackTitle", "black_titled"},
    {"WhiteElo", "white_elo"},
    {"BlackElo", "black_elo"},
    {"Result", "result"},
    {"WhiteRatingDiff", "white_rating_diff"},
    {"BlackRatingDiff", "black_rating_diff"},
    {"TimeControl", "time_control"},
    {"Termination", "termination"},
    {"ECO", "eco"},
    {"Opening", "opening"},
    {"w_move", "white_n_moves"},
    {"b_move", "black_n_moves"},
    {"player_move", "player_n_moves"},
    {"move", "moves"}
};


void prevent_sleep() {
#ifdef _WIN32
    SetThreadExecutionState(
        ES_CONTINUOUS | ES_SYSTEM_REQUIRED | ES_DISPLAY_REQUIRED);
#endif
}

class Logger {
private:
    std::ofstream file;

    std::string timestamp() {
        auto now = std::chrono::system_clock::now();
        auto t = std::chrono::system_clock::to_time_t(now);

        std::ostringstream ss;
        ss << std::put_time(std::localtime(&t), "%Y-%m-%d %H:%M:%S");
        return ss.str();
    }

public:
    Logger(const std::filesystem::path& path) {
        fs::path log_path = BASE_PATH / path;
        std::filesystem::create_directories(log_path.parent_path());
        file.open(log_path, std::ios::app);
        if (!file) {
            throw std::runtime_error("Cannot open log file");
        }
    }

    void info(const std::string& msg, bool also_console = false) {
        std::string full =
            "[" + timestamp() + "] [INFO] " + msg;

        file << full << "\n";

        if (also_console) {
            std::cerr << full << std::endl;
        }
    }
};

string log_progress(long scanned, long collected, long total, std::chrono::steady_clock::time_point start_time) {

    auto now = steady_clock::now();
    double elapsed_sec = duration_cast<seconds>(now - start_time).count();

    double speed = scanned > 0 ? scanned / elapsed_sec : 0.0;
    double pct = (double)scanned / total * 100.0;
    double eta_sec = speed > 0 ? (total - scanned) / speed : 0.0;

    std::ostringstream oss;
    oss << "Scanned: " << scanned
        << " | Collected: " << collected
        << " | " << fixed << setprecision(2) << pct << "%"
        << " | Speed: " << fixed << setprecision(0) << speed << " g/s"
        << " | Elapsed: " << (long)elapsed_sec << " s"
        << " | ETA: " << (long)eta_sec << " s";
    
    return oss.str();
}

struct OrderedDict {
    std::unordered_map<std::string, std::string> data;
    std::vector<std::string> order;

    void insert(const std::string &key, const std::string &value) {
        if (data.find(key) == data.end()) {
            order.push_back(key);
        }
        data[key] = value;
    }
};

OrderedDict parse_game_header(const string &raw_game) {

    OrderedDict game;
    std::stringstream ss(raw_game);
    std::string header_line;

    while (std::getline(ss, header_line)) {
        if (header_line.empty()) break; // header selesai

        size_t start_key = header_line.find('[');
        size_t end_key   = header_line.find(' ', start_key);
        size_t start_val = header_line.find('"', end_key);
        size_t end_val   = header_line.rfind('"');

        if (start_key != std::string::npos &&
            end_key   != std::string::npos &&
            start_val != std::string::npos &&
            end_val   != std::string::npos) {

            // assign key value
            std::string key   = header_line.substr(start_key + 1, end_key - start_key - 1);
            std::string value = header_line.substr(start_val + 1, end_val - start_val - 1);
            game.insert(key, value);
        }
    }

    return game;
}

OrderedDict parse_game_moves(const OrderedDict &game_header, const string &raw_game) {
    OrderedDict game = game_header;
    std::string line;
    std::string moves_str;
    std::stringstream ss(raw_game);

    while (std::getline(ss, line)) {
        if (!line.empty() && line[0] == '[') continue;
        moves_str += line + " ";
    }

    // cleaning moves
    moves_str = std::regex_replace(moves_str, std::regex(R"(\{.*?\})"), "");
    moves_str = std::regex_replace(moves_str, std::regex(R"(\d+\.\.\.)"), ",");
    moves_str = std::regex_replace(moves_str, std::regex(R"(\d+\.)"), ".");
    moves_str = std::regex_replace(moves_str, std::regex(R"(\s*(1-0|0-1|1/2-1/2)\s*$)"), "");
    moves_str = std::regex_replace(moves_str, std::regex(R"([!?])"), "");
    moves_str = std::regex_replace(moves_str, std::regex(R"(\s+)"), "");

    // count moves
    int w_count = moves_str.empty()
        ? 0
        : std::count(moves_str.begin(), moves_str.end(), '.');

    int b_count = moves_str.empty()
        ? 0
        : std::count(moves_str.begin(), moves_str.end(), ',');

    // cleaning moves
    moves_str = std::regex_replace(moves_str, std::regex(R"(\,)"), " ");
    moves_str = std::regex_replace(moves_str, std::regex(R"(\.)"), " ");
    moves_str = std::regex_replace(moves_str, std::regex(R"(^\s+)"), "");

    // assign key value
    game.insert("move", moves_str);
    game.insert("w_move", std::to_string(w_count));
    game.insert("b_move", std::to_string(b_count));
    game.insert("player_move", std::to_string(w_count + b_count));

    return game;
}

bool filter_game(const OrderedDict &game_header, int min_elo = MIN_ELO) {
    auto w_elo = game_header.data.find("WhiteElo");
    auto b_elo = game_header.data.find("BlackElo");
    auto t_con = game_header.data.find("TimeControl");

    // cek apakah data ada di dalam dictionary game
    if (w_elo == game_header.data.end() || b_elo == game_header.data.end() || t_con == game_header.data.end()){
        return false;
    }

    try {
        int white_elo = std::stoi(w_elo->second);
        int black_elo = std::stoi(b_elo->second);

        // filter elo
        if (white_elo < min_elo || black_elo < min_elo){
            return false;
        }

        const std::string &time_con = t_con->second; 
        size_t plus_sign = time_con.find('+');
        int base_time = std::stoi(time_con.substr(0, plus_sign));

        // filter time control
        if (base_time < 180 || base_time > 300){
            return false;
        }

        return true;

    } catch (...) {
        return false;
    }
};

OrderedDict normalize_to_schema(const OrderedDict &raw_game, const Schema &schema) {
    OrderedDict out;

    for (const auto &[pgn_key, csv_key] : schema) {

        auto raw_key_value = raw_game.data.find(pgn_key);

        if (raw_key_value != raw_game.data.end()) {
            out.insert(csv_key, raw_key_value->second);
        } else {
            out.insert(csv_key, "");  // kosong jika tidak ada
        }
    }

    return out;
}

struct CSVWriter {
    std::ofstream fout;
    bool header_written = false;

    CSVWriter(const fs::path &filename) {
        fout.open(filename);
        if (!fout) {
            throw std::runtime_error("Cannot open CSV file");
        }
    }

    void write_game(const OrderedDict &game) {
        // tulis header sekali
        if (!header_written) {
            for (size_t i = 0; i < game.order.size(); ++i) {
                fout << game.order[i];
                if (i + 1 < game.order.size()) fout << ",";
            }
            fout << "\n";
            header_written = true;
        }

        // tulis row
        for (size_t i = 0; i < game.order.size(); ++i) {
            const std::string &key = game.order[i];
            std::string value = game.data.at(key);

            // escape double quote
            size_t pos = 0;
            while ((pos = value.find('"', pos)) != std::string::npos) {
                value.insert(pos, "\"");
                pos += 2;
            }

            fout << "\"" << value << "\"";
            if (i + 1 < game.order.size()) fout << ",";
        }
        fout << "\n";
    }
};

void flush_batch_to_csv(std::vector<OrderedDict> &batch, CSVWriter &csv) {
    for (const auto &game : batch) {
        csv.write_game(game);
    }
    batch.clear();  // kosongkan batch
}


int main() {
    prevent_sleep();

    Logger logger("logs/parser.log");
    logger.info("======================================");
    logger.info("Starting PGN parsing...", true);
    auto start_time = std::chrono::steady_clock::now();
    
    const fs::path file_path = SOURCE_PATH;
    CSVWriter csv_target(OUTPUT_PATH);

    // Baca file .zst secara streaming
    ifstream fin(file_path, ios::binary);
    if (!fin) {
        cerr << "[ERROR] Cannot open file: " << file_path << endl;
        return 1;
    }

    // Bisa dekompres chunk per chunk
    ZSTD_DStream* dstream = ZSTD_createDStream();
    ZSTD_initDStream(dstream);
    if (!dstream) {
        cerr << "[ERROR] Cannot create ZSTD context\n";
        return 1;
    }

    const size_t IN_BUF_SIZE = ZSTD_DStreamInSize();
    const size_t OUT_BUF_SIZE = ZSTD_DStreamOutSize();

    vector<char> inBuf(IN_BUF_SIZE);
    vector<char> outBuf(OUT_BUF_SIZE);

    vector<OrderedDict> all_games;  // menyimpan seluruh game
    string current_game;

    size_t scanned_games = 0;
    size_t collected_games = 0;
    
    bool game_finished = false;

    while (collected_games < games_to_read && fin) {
        fin.read(inBuf.data(), inBuf.size());
        size_t bytes_read = fin.gcount();
        if (bytes_read == 0) break;

        ZSTD_inBuffer input{ inBuf.data(), bytes_read, 0 };

        while (input.pos < input.size && collected_games < games_to_read) {

            ZSTD_outBuffer output{ outBuf.data(), outBuf.size(), 0 };
            size_t ret = ZSTD_decompressStream(dstream, &output, &input);
            
            if (ZSTD_isError(ret)) {
                cerr << "[ERROR] ZSTD decompress error: "
                     << ZSTD_getErrorName(ret) << endl;
                return 1;
            }

            string chunk(outBuf.data(), output.pos);
            stringstream ss(chunk);
            string line;
            
            while (getline(ss, line)) {
                if (line.rfind("[Event ", 0) == 0) {  // masuk setiap game ketika awal nya "[Event"

                    if (!current_game.empty()) {  // pengecekan di game pertama

                        OrderedDict game_header = parse_game_header(current_game);
                        // all_games.push_back(game_header);

                        // FILTER GAME YANG SESUAI
                        if (filter_game(game_header)){
                            OrderedDict game = parse_game_moves(game_header, current_game);
                            OrderedDict safe_csv_game = normalize_to_schema(game, CSV_SCHEMA);
                            all_games.push_back(safe_csv_game);
                            collected_games++;
                            
                            if (all_games.size() >= BATCH_SIZE) {
                                flush_batch_to_csv(all_games, csv_target);
                            }
                        } 

                        current_game.clear();
                        scanned_games++;
                        
                        if (scanned_games % LOG_CHECKPOINT == 0){
                            string progress = log_progress(scanned_games, collected_games, TOTAL_GAMES, start_time);
                            cerr << "\r[INFO] " << progress << std::flush;
                        }

                        if (collected_games >= games_to_read) {
                            cout << "\n\nTOTAL GAMES :" << collected_games << "\n";
                            break;
                        }
                    }
                }

                current_game += line;
                current_game += "\n";
            }
        }
    }

    ZSTD_freeDStream(dstream);

    // tampilkan
    // for (size_t i = 0; i < all_games.size(); ++i) {

    //     cout << "\n===== GAME " << (i + 1) << " =====\n";
    //     const auto &game = all_games[i];

    //     for (const auto &key : game.order) {
    //         std::cout << key << " : " << game.data.at(key) << "\n";
    //     }
    // }

    // logging
    auto end_time = std::chrono::steady_clock::now();
    auto total_sec =
        std::chrono::duration_cast<std::chrono::seconds>(end_time - start_time).count();

    logger.info("Run finished    : " + FILE_NAME.string(), true);
    logger.info("Total scanned   : " + std::to_string(scanned_games), true);
    logger.info("Total collected : " + std::to_string(collected_games), true);
    logger.info("Total time      : " + std::to_string(total_sec) + " seconds", true);
    logger.info("Summary         : " + log_progress(scanned_games, collected_games, TOTAL_GAMES, start_time), true);
    logger.info("======================================\n");

    return 0;
}
