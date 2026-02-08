#include <zstd.h>
#include <fstream>
#include <iostream>
#include <vector>
#include <string>
#include <sstream>
#include <unordered_map>
#include <regex>



using namespace std;

static const int games_to_read = 10;  // jumlah game yang ingin dilihat


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

OrderedDict parse_single_game(const std::string &game_raw) {

    OrderedDict game;
    std::stringstream ss(game_raw);

    /* ================= HEADER ================= */
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

    /* ================= MOVES ================= */
    std::string moves_line;
    std::string moves_str;

    while (std::getline(ss, moves_line)) {
        moves_str += moves_line + " ";
    }

    // cleaning moves
    moves_str = std::regex_replace(moves_str, std::regex(R"(\{.*?\})"), "");
    moves_str = std::regex_replace(moves_str, std::regex(R"(\d+\.\.\.)"), ",");
    moves_str = std::regex_replace(moves_str, std::regex(R"(\d+\.)"), ".");
    moves_str = std::regex_replace(moves_str, std::regex(R"(\s*(1-0|0-1|1/2-1/2)\s*$)"), "");
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

bool filter(const OrderedDict &game, int min_elo = 2000) {
    auto w_elo = game.data.find("WhiteElo");
    auto b_elo = game.data.find("BlackElo");
    auto t_con = game.data.find("TimeControl");

    // cek apakah data ada di dalam dictionary game
    if (w_elo == game.data.end() || b_elo == game.data.end() || t_con == game.data.end()){
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
        if (base_time < 600 || base_time > 1800){
            return false;
        }

        return true;

    } catch (...) {
        return false;
    }
};

struct CSVWriter {
    std::ofstream fout;
    bool header_written = false;

    CSVWriter(const std::string &filename) {
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


int main() {
    const string file_path = "C:/Users/gagah/Documents/Portofolios/Chess-analysis/lichess_db_standard_rated_2025-12.pgn.zst";
    CSVWriter csv_target("C:/Users/gagah/Documents/Portofolios/Chess-analysis/data/test_cpp_lichess_rapid_elo2000.csv");

    // Baca file .zst secara streaming
    ifstream fin(file_path, ios::binary);
    if (!fin) {
        cerr << "[ERROR] Cannot open file: " << file_path << endl;
        return 1;
    }

    // Bisa dekompres chunk per chunk
    ZSTD_DCtx* dctx = ZSTD_createDCtx();
    if (!dctx) {
        cerr << "[ERROR] Cannot create ZSTD context\n";
        return 1;
    }

    const size_t IN_BUF_SIZE = ZSTD_DStreamInSize();
    const size_t OUT_BUF_SIZE = ZSTD_DStreamOutSize();

    vector<char> inBuf(IN_BUF_SIZE);
    vector<char> outBuf(OUT_BUF_SIZE);

    std::vector<OrderedDict> all_games;  // menyimpan seluruh game
    string temp_game;
    string current_game;
    
    bool game_finished = false;

    while (all_games.size() < games_to_read && fin) {
        fin.read(inBuf.data(), inBuf.size());
        size_t bytes_read = fin.gcount();
        if (bytes_read == 0) break;

        ZSTD_inBuffer input{ inBuf.data(), bytes_read, 0 };

        while (input.pos < input.size && all_games.size() < games_to_read) {

            ZSTD_outBuffer output{ outBuf.data(), outBuf.size(), 0 };
            size_t ret = ZSTD_decompressStream(dctx, &output, &input);
            
            if (ZSTD_isError(ret)) {
                cerr << "[ERROR] ZSTD decompress error: "
                     << ZSTD_getErrorName(ret) << endl;
                return 1;
            }

            for (size_t i = 0; i < output.pos; ++i) {
                char c = outBuf[i];
                current_game.push_back(c);

                // jika menemukan "[Event " dan current_game sudah punya data
                if (current_game.size() > 7 && current_game.substr(current_game.size()-7) == "[Event ") {
                    // simpan game sebelumnya
                    if (!all_games.empty() || current_game.size() > 7) {

                        temp_game = current_game.substr(0, current_game.size()-7);
                        OrderedDict game = parse_single_game(temp_game);

                        if(filter(game)){
                            all_games.push_back(game);
                            csv_target.write_game(game);
                        }
                        current_game = "[Event "; // mulai game baru
                    }
                }
                if (all_games.size() >= games_to_read) break;
            }
        }
    }

    ZSTD_freeDCtx(dctx);

    // tampilkan
    for (size_t i = 0; i < all_games.size(); ++i) {

        cout << "\n===== GAME " << (i + 1) << " =====\n";
        const auto &game = all_games[i];

        for (const auto &key : game.order) {
            std::cout << key << " : " << game.data.at(key) << "\n";
        }
    }

    return 0;
}
