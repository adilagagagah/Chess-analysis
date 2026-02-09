#include <zstd.h>
#include <fstream>
#include <iostream>
#include <vector>
#include <string>

#include <sstream>
#include <unordered_map>
#include <regex>



using namespace std;

static const int games_to_read = 3;  // jumlah game yang ingin dilihat


int main() {
    const string file_path = "C:/Users/gagah/Documents/Portofolios/Chess-Analysis/lichess_db_standard_rated_2025-12.pgn.zst";

    ifstream fin(file_path, ios::binary);
    if (!fin) {
        cerr << "[ERROR] Cannot open file: " << file_path << endl;
        return 1;
    }

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

    vector<string> raw_game;
    vector<string> all_games;
    vector<string> games;   // menyimpan beberapa game
    string current_game;
    
    bool game_finished = false;
    int scanned_games = 0;
    int blank = 0;

    while (games.size() < games_to_read && fin) {
        fin.read(inBuf.data(), inBuf.size());
        size_t bytes_read = fin.gcount();
        if (bytes_read == 0) break;

        ZSTD_inBuffer input{ inBuf.data(), bytes_read, 0 };

        while (input.pos < input.size && games.size() < games_to_read) {
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

            while (getline(ss, line)){                
                if(line.empty() && !current_game.empty()){
                    blank++;

                    if(blank % 2 == 0){
                        scanned_games++;

                        cout << "===== GAME " << (scanned_games) << " =====\n";
                        cout << current_game;

                        // filter

                        // parse single game => dict single game

                        // convert column to csv schema

                        // add to vektor batch (all_games)

                        // logging progress

                        // after reached batch number, convert to csv



                        current_game.clear();
                    }
                } else {
                    current_game += line;
                    current_game += "\n";
                    // raw_game.push_back(line);
                }

                if (scanned_games >= games_to_read){
                    cout << "total games :" << games_to_read << "\n";

                    return 1;
                }
            }
        }
    }

    ZSTD_freeDStream(dstream);
    
    return 0;
}
