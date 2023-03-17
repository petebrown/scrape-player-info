from bs4 import BeautifulSoup
import pandas as pd
import requests
import concurrent.futures

MAX_THREADS = 30

def construct_url(team_id, season_id):
    url = f'https://www.soccerbase.com/teams/team.sd?team_id={team_id}&teamTabs=stats&season_id={season_id}'
    return url

def get_season_urls():
    team_id = 2598
    season_id = 155

    url = construct_url(team_id, season_id)
    r = requests.get(url)
    doc = BeautifulSoup(r.text, 'html.parser')
    
    season_list = doc.select('#statsSeasonSelectTop option')
    season_ids = [construct_url(team_id, season["value"]) for season in season_list[1:]]

    return season_ids

def get_player_list(url):
    session = requests.Session()
    r = session.get(url)
    doc = BeautifulSoup(r.text, 'html.parser')
    
    season = doc.select_one('.seasonSelector h3').text
    player_list = doc.select('table.center tbody tr')

    all_players = []
    for player in player_list:
        player_info = player.select_one('.first')

        player_name = player_info.get_text()
        player_name = player_name.split('(')
        player_name = player_name[0]
        player_name = player_name.strip()
    
        player_url = player_info.select_one('a')['href']
        player_url = f"https://www.soccerbase.com{player_url}"

        player_id = player_url.split("=")[1]

        all_players.append({
            "player_id": player_id,
            "player_name": player_name,
            "player_url": player_url
        })
    return all_players

def get_player_details(url):
    session = requests.Session()
    r = session.get(url)
    doc = BeautifulSoup(r.text, 'html.parser')

    player_id = url.split("=")[1]

    player_position = doc.select_one((".midfielder.bull"))

    if player_position:
        player_position = player_position.text.strip().split(" ")[0]
    else:
        player_position = "NA"

    info_1 = pd.read_html(r.text)[1]
    info_2 = pd.read_html(r.text)[2]
    df = pd.concat([info_1, info_2])
    df["player_id"] = player_id

    df = df.dropna().pivot(index=["player_id"], columns = [0], values = [1]).reset_index()
    df.columns = df.columns.droplevel([None]).str.lower().str.replace(" ", "_")
    player_record = df.to_dict(orient="records")[0]
    player_record["player_position"] = player_position

    return player_record

def async_scraping(scrape_function, urls):
    threads = min(MAX_THREADS, len(urls))
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        results = executor.map(scrape_function, urls)

    return results

def add_position(player_id):
    if player_id in ["5682", "3482", "7940", "7606", "4316"]:
        position = "Defender"
    elif player_id in ["111841"]:
        position = "Midfielder"
    elif player_id in ["9811"]:
        position = "Forward"
    else:
        position = "NA"
    return position

def insert_manual_updates(df):
    updates = pd.read_csv("./manual_updates.csv", parse_dates=["player_dob"])

    df.player_id = df.player_id.astype(int)
    df = df[~df.player_id.isin(updates.player_id)]
    df = pd.concat([df, updates]).reset_index(drop=True)

    return df

def main():
    season_urls = get_season_urls()    

    player_list = async_scraping(get_player_list, season_urls)
    player_list = list(player_list)
    player_list = [player for sublist in player_list for player in sublist]

    player_urls = [player["player_url"] for player in player_list]

    player_info = async_scraping(get_player_details, player_urls)
    player_info = list(player_info)

    df = pd.DataFrame(player_info).drop_duplicates()
    df["player_position"] = df.apply(lambda x: add_position(x.player_id) if x.player_position == "NA" else x.player_position, axis=1)
    df["player_dob"] = df.age.str.split("Born ").str[1].str.split(")").str[0]
    df["player_dob"] = pd.to_datetime(df.player_dob)
    df["date_signed"] = pd.to_datetime(df.date_signed)
    
    df["height_ft"] = df.height.str.split(" \(").str[0]
    df["height_cm"] = df.height.str.split(" \(").str[1].str.split("m").str[0].astype("float") * 100
    df["weight_st"] = df.weight.str.split(" \(").str[0]
    df["weight_kg"] = df.weight.str.split(" \(").str[1].str.split("kg").str[0].astype("float")

    cols_to_keep = ["player_id", "real_name", "player_dob", "player_position", "place_of_birth", "nationality", "height_ft", "height_cm", "weight_st", "weight_kg"]
    df = df[cols_to_keep]

    df = insert_manual_updates(df)
    
    return df

df = main()

df.to_csv("./data/player-info.csv", index=False)