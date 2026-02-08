#include <zstd.h>
#include <fstream>
#include <iostream>
#include <vector>
#include <string>
#include <sstream>
#include <unordered_map>
#include <regex>



using namespace std;

static const int games_to_read = 2;  // jumlah game yang ingin dilihat


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
    std::string header_line;

    /* ================= HEADER ================= */
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
    moves_str = std::regex_replace(moves_str, std::regex(R"(\d+\.\.\.)"), "");
    moves_str = std::regex_replace(moves_str, std::regex(R"(\d+\.)"), ".");
    moves_str = std::regex_replace(moves_str, std::regex(R"(\s*(1-0|0-1|1/2-1/2)\s*$)"), "");
    moves_str = std::regex_replace(moves_str, std::regex(R"(\s+)"), "");
    moves_str = std::regex_replace(moves_str, std::regex(R"(\.)"), " ");
    moves_str = std::regex_replace(moves_str, std::regex(R"(^\s+)"), "");

    game.insert("move", moves_str);

    /* ================= COUNT MOVES ================= */
    int num_moves = moves_str.empty()
        ? 0
        : std::count(moves_str.begin(), moves_str.end(), ' ') + 1;

    game.insert("num_move", std::to_string(num_moves));

    return game;
}


int main() {
    const string file_path = "C:/Users/gagah/Documents/Portofolios/Chess-analysis/lichess_db_standard_rated_2025-12.pgn.zst";

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

    std::vector<OrderedDict> all_games;  // menyimpan beberapa game
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

                        OrderedDict game = parse_single_game(current_game.substr(0, current_game.size()-7));
                        all_games.push_back(game);
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
