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


int main() {
    const string file_path = "C:/Users/gagah/Documents/Portofolios/Chess-Analysis/lichess_db_standard_rated_2025-12.pgn.zst";

    ifstream fin(file_path, ios::binary);
    if (!fin) {
        cerr << "[ERROR] Cannot open file: " << file_path << endl;
        return 1;
    }

    ZSTD_DCtx* dctx = ZSTD_createDCtx();
    if (!dctx) {
        cerr << "[ERROR] Cannot create ZSTD context\n";
        return 1;
    }

    const size_t IN_BUF_SIZE = ZSTD_DStreamInSize();
    const size_t OUT_BUF_SIZE = ZSTD_DStreamOutSize();

    vector<char> inBuf(IN_BUF_SIZE);
    vector<char> outBuf(OUT_BUF_SIZE);

    std::vector<string> all_games;
    vector<string> games;   // menyimpan beberapa game
    string current_game;
    
    bool game_finished = false;

    while (games.size() < games_to_read && fin) {
        fin.read(inBuf.data(), inBuf.size());
        size_t bytes_read = fin.gcount();
        if (bytes_read == 0) break;

        ZSTD_inBuffer input{ inBuf.data(), bytes_read, 0 };

        while (input.pos < input.size && games.size() < games_to_read) {
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
                    if (!games.empty() || current_game.size() > 7) {
                        games.push_back(current_game.substr(0, current_game.size()-7));
                        current_game = "[Event "; // mulai game baru
                    }
                }
                if (games.size() >= games_to_read) break;
            }
        }
    }

    ZSTD_freeDCtx(dctx);
    
    
    cout << "\n\n[DEBUG] DICTIONARY & CLEAN GAME:" << "\n";
    // tampilkan semua game yang berhasil dibaca
    for (size_t i = 0; i < games.size(); ++i) {
        cout << "\n===== GAME " << (i + 1) << " =====\n";
        cout << games[i] << "\n";

        std::vector<std::string> header_order;
        std::unordered_map<std::string, std::string> game_dict;

        std::stringstream ss(games[i]);
        std::string line;
        std::string moves_str;

        // parsing header
        while (std::getline(ss, line)) {
            if (line.empty()) break; // header berakhir saat empty line

            // cari pattern [Key "Value"]
            size_t start_key = line.find('[');
            size_t end_key = line.find(' ', start_key);
            size_t start_val = line.find('"', end_key);
            size_t end_val = line.rfind('"');

            if (start_key != std::string::npos && end_key != std::string::npos &&
                start_val != std::string::npos && end_val != std::string::npos) {
                std::string key = line.substr(start_key + 1, end_key - start_key - 1);
                std::string value = line.substr(start_val + 1, end_val - start_val - 1);
                // simpan di dictionary
                if (game_dict.find(key) == game_dict.end()) {
                    header_order.push_back(key); // simpan urutan
                }
                game_dict[key] = value;
            }
        }

        // parsing move
        std::string moves_line;
        while (std::getline(ss, moves_line)) {
            moves_str += moves_line + " ";
        }

        // hapus nomor langkah dan waktu
        std::regex dots_regex(R"(\d+\.\.\.)");
        moves_str = std::regex_replace(moves_str, dots_regex, ","); // hapus titik langkah
        std::regex number_regex(R"(\d+\.)");
        moves_str = std::regex_replace(moves_str, number_regex, "."); // hapus angka langkah
        std::regex comment_regex(R"(\{.*?\})");
        moves_str = std::regex_replace(moves_str, comment_regex, ""); // hapus komentar
        std::regex result_regex(R"(\s*(1-0|0-1|1/2-1/2)\s*$)");
        moves_str = std::regex_replace(moves_str, result_regex, ""); // hapus result
        std::regex sign_regex(R"([!?])");
        moves_str = std::regex_replace(moves_str, sign_regex, "");
        std::regex extra_spaces(R"(\s+)");
        moves_str = std::regex_replace(moves_str, extra_spaces, ""); // rapikan spasi

        int w_count = std::count(moves_str.begin(), moves_str.end(), '.');
        game_dict["w_move"] = std::to_string(w_count);

        int b_count = std::count(moves_str.begin(), moves_str.end(), ',');
        game_dict["b_move"] = std::to_string(b_count);

        game_dict["player_move"] = std::to_string(w_count + b_count);
        
        moves_str = std::regex_replace(moves_str, std::regex(R"(\.)"), " ");
        moves_str = std::regex_replace(moves_str, std::regex(R"(\,)"), " ");
        moves_str = std::regex_replace(moves_str, std::regex(R"(^\s+)"), "");

        game_dict["move"] = moves_str;

        // simpan urutan
        header_order.push_back("move");
        header_order.push_back("w_move");
        header_order.push_back("b_move");
        header_order.push_back("player_move");

        // tampilkan
        for (const auto &key : header_order) {
            std::cout << key << " : " << game_dict[key] << "\n";
        }

    }


    return 0;
}
