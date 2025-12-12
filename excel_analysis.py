import pandas as pd
import re



def is_size(s: str) -> bool:
    return bool(size_pattern.match(s))

# 5. Expand available_sizes_in_request into exploded table (helper)
def split_sizes(s):
    if pd.isna(s) or s == "" or s.lower() == "nan":
        return []
    return [x.strip() for x in str(s).split(",") if x.strip()]


# 1 load data
df = pd.read_excel('xlsx_files\Data_Analysis_Programmatic_Operations_Manager.xlsx', sheet_name='Sheet1')

# 2 basic schema validation
expected_cols = {"ssp_id", "bidlog_status", "bid_adformat", "available_sizes_in_request", "hits"}
missing = expected_cols - set(df.columns)
print('Start data validation')
if missing:
    raise ValueError(f"Missing columns: {missing}")
else:
    print("All expected columns are present.")

#  -------------------------
print(f'\nLiczba wierszy w pliku: {len(df)}')
# DF has 895 rows
#  -------------------------
# add col with assign row number to achieve verification of delete empty rows and other operations.
df["lp"] = range(1, len(df) + 1)
#  -------------------------
# detele empty rows in all columns.
df = df.dropna(how="all").copy()
print(f'\nRepeat validation amount of rows after delete empty rows {len(df)}, nothing change, data set had no empty rows')
# -------------------------- Section of data exploration --------------------------
# next line presents unique values in particular columns to understand data better
print(df["ssp_id"].unique())
# result > [ 8  5  2  9  6  3  7 10]
#  -------------------------
print(df["bidlog_status"].unique())
# Result ['BID_OK' 'TIMEOUT' 'NO_BID' 'NOT_LEGAL'] same like in spec.
#  -------------------------
print('list of list of formats/sizes represents all possible format inside request sent to monetise')
print(df["bid_adformat"].unique())
print('-------------------------')
print('available_sizes_in_request\n', df["available_sizes_in_request"].unique())
print('-------------------------')
# Conclusion 1 for solution point 1:
# To get information about the most popular sizes, you need to split the rows and count the data for each format separately.
# If there was a request with multiple formats, it means you could specify a given format and bid on it, so each one should be counted separately.
# -------------------------- End of section data exploration --------------------------

# -------------------------- Section of data cleaning --------------------------
# Normalisation of data - trim spaces and convert to string
df["bidlog_status"] = df["bidlog_status"].str.strip()
df["bid_adformat"] = df["bid_adformat"].astype(str).str.strip()
df["available_sizes_in_request"] = df["available_sizes_in_request"].astype(str).str.strip()
# -------------------------- End of section data cleaning --------------------------

#  ------------------------- Section Dupplicate check --------------------------
# find duplicates with all same values in key columns
duplicates = df[
    df.duplicated(subset=["ssp_id", "bidlog_status", "bid_adformat", "available_sizes_in_request", "hits"], keep=False)
].sort_values(["available_sizes_in_request", "hits"])
duplicates.to_excel("duplicates.xlsx", index=False)

print('table duplicates to watch how it looks like\n', duplicates)
print(f"rows  {len(duplicates)}")

# detele exactly same duplicates (keep first exist )
df_cleaned_from_duplicates = df.drop_duplicates(subset=["ssp_id", "bidlog_status", "bid_adformat", "available_sizes_in_request", "hits"])
df_cleaned_from_duplicates.to_excel("df_cleaned_from_duplicates.xlsx", index=False)
# reasign lp after delete duplicates
df_cleaned_from_duplicates["lp"] = range(1, len(df_cleaned_from_duplicates) + 1)
df = df_cleaned_from_duplicates
print('count rows number after delete duplicates \n', len(df))
print('-------------------------')
# -------------------------- End of section Dupplicate check -------------------
#  ------------------------- Section Nan check --------------------------


# --------------------------
# save df to check in excel after first part validation
df.to_excel("df_after_validation.xlsx", index=False)
#  -------------------------

# -------------------------- Section df manipulation and cleaning --------------------------
# 4. Flag valid ad size pattern (w x h) or 'n/a'

# explenation regex pattern code
# Creates regex pattern ^\d+x\d+$ which checks if string has "number x number" format:
# ^ - start of string
# \d+ - one or more digits
# x - exactly letter "x"
# \d+ - one or more digits
# $ - end of string
# re.IGNORECASE - ignores case (uppercase/lowercase)
size_pattern = re.compile(r"^\d+x\d+$", re.IGNORECASE)

requests_exploded = (
    df
    .assign(size_list=df["available_sizes_in_request"].apply(split_sizes))
    .explode("size_list")
    .rename(columns={"size_list": "available_size"})
)
print('new requests_exploded df represent original df spread vertically by each size inside request to allow '
      'achieve answer to question number 1'
      ' \n', requests_exploded)
print('-------------------------')
# -------------------------- End of section df manipulation and cleaning --------------------------
#  -------------------------
print('additional check how number of rows changed. ', len(requests_exploded))
print('-------------------------')
#  -------------------------
print('it shows all possible sizes inside request sent to monetise: \n', requests_exploded["available_size"].unique())
print('-------------------------')
#  -------------------------
# save with seperated sizes to check in excel
requests_exploded.to_excel("df_after_separation_via_formats.xlsx", index=False)
#  -------------------------
# keep only validated sizes; ignore 'n/a' etc.
requests_exploded = requests_exploded[requests_exploded["available_size"].apply(is_size)]
print('grouped data per avialable size to answer question 1')
inventory = (
    requests_exploded
    .groupby("available_size", as_index=False)["hits"]
    .sum()
    .sort_values("hits", ascending=False)
)
print('data frame with groped data per available size and summed hits', inventory, '\n')
print('-------------------------')
# ------------------------- response question 1 -------------------------
print('# ------------------------- response question 1 -------------------------')
top3_inventory = inventory.head(3)
print('Answer 1: top 3 inventory formats: \n', top3_inventory)
print('-------------------------')
# ------------------------- end of response question 1 -------------------------

# total auctions per SSP (weighted by hits)
ssp_total = df.groupby("ssp_id", as_index=False)["hits"].sum().rename(columns={"hits": "hits_total"})
print('ssp total means all possible request to buy: \n', ssp_total)
df.groupby("ssp_id", as_index=False)["hits"].sum().rename(columns={"hits": "hits_total"})

# successful bids per SSP
ssp_bidok = (
    df[df["bidlog_status"] == "BID_OK"]
    .groupby("ssp_id", as_index=False)["hits"]
    .sum()
    .rename(columns={"hits": "hits_bid_ok"})
)
print('df grouped per ssp with bids status ok:\n', ssp_bidok)
# -------------------------
# merge to connect bids ok won with all possible requests to buy
ssp_rates = (
    ssp_total
    .merge(ssp_bidok, on="ssp_id", how="left")
    .fillna({"hits_bid_ok": 0})
)
# calculate bid rate
ssp_rates["bid_rate"] = ssp_rates["hits_bid_ok"] / ssp_rates["hits_total"]
# prepare highest and lowest bid rate ssps
ssp_highest = ssp_rates.sort_values("bid_rate", ascending=False).head(1)
ssp_lowest = ssp_rates.sort_values("bid_rate", ascending=True).head(1)

# ------------------------- Response question 2 -------------------------
print('# ------------------------- response question 2 -------------------------')
print("Highest bid rate:\n", ssp_highest)
print("Lowest bid rate:\n", ssp_lowest)
# ------------------------- end of response question 2 -------------------------

# Filter > SSP chosen sizes (bid status BID_OK or TIMEOUT)
chosen = (
    df[df["bidlog_status"].isin(["BID_OK", "TIMEOUT"])]
    .copy()
)

# filter out rows with 'n/a' / empty bid_adformat, acceptable size is "WxH"
chosen = chosen[chosen["bid_adformat"].apply(is_size)]

# grouping: which size SSP most often CHOOSES
ssp_size_hits = (
    chosen
    .groupby(["ssp_id", "bid_adformat"], as_index=False)["hits"]
    .sum()
    .rename(columns={"bid_adformat": "chosen_size", "hits": "hits_chosen"})
)
print('--------------------------')
print('all possible volumen per size per SSP to buy\n', ssp_size_hits)
print('--------------------------')
# df_total_amount_bid_per_SSP_per_Size
ssp_size_hits.to_excel("df_total_amount_bid_per_SSP_per_Size.xlsx", index=False)

# ------------------------- Response question 3 -------------------------
print(' ------------------------- Response question 3 -------------------------')
# Top format per SSP
ssp_top_size = (
    ssp_size_hits
    .sort_values(["ssp_id", "hits_chosen"], ascending=[True, False])
    .groupby("ssp_id", as_index=False)
    .head(1)
)

print('TOP 1 from all possible volumen per SSP per size to buy MEANS most choosen size per SSP\n', ssp_top_size)
print('--------------------------')
# df_total_amount_bid_per_SSP_per_Size_winning_sizes_top_1
ssp_top_size.to_excel("df_total_amount_bid_per_SSP_per_Size_winning_sizes_top_1.xlsx", index=False)
# ------------------------- End response question 3 -------------------------
inventory_size = (
    inventory
    .rename(columns={"available_size": "ad_size", "hits": "hits_inventory"})
)
print('all possible requests per ad_size (capacity per size)\n', inventory_size.head())
print('--------------------------')
# --------------------------
# Wins per size (where DSP actually bid with that size and status is BID_OK)
wins_size = (
    df[(df["bidlog_status"] == "BID_OK") & df["bid_adformat"].apply(is_size)]
    .groupby("bid_adformat", as_index=False)["hits"]
    .sum()
    .rename(columns={"bid_adformat": "ad_size", "hits": "hits_won"})
)
print('Wins per size (where DSP actually bid with that size and status is BID_OK)\n', wins_size.head(20))
print('------------------------- Response question 4 -------------------------')
# join data to calculate opportunity loss per size
df_joined = (
    inventory_size
    .merge(wins_size, on="ad_size", how="left")
    .fillna({"hits_won": 0})
)
# calculate opportunity loss
df_joined["opportunity_loss"] = (df_joined["hits_inventory"] - df_joined["hits_won"]).clip(lower=0)
#calculate opportunity loss percentage
df_joined["opportunity_loss_perc"] = ((df_joined["hits_inventory"] - df_joined["hits_won"])/df_joined["hits_inventory"] * 100).clip(lower=0)
# get top 3 sizes with highest opportunity loss
top3_opp_loss = df_joined.sort_values("opportunity_loss", ascending=False).head(3)
print('top3 opportunity loss \n', top3_opp_loss)
top3_opp_loss_perc = df_joined.sort_values("opportunity_loss_perc", ascending=False).head(3)
print('top3 opportunity loss perc \n', top3_opp_loss_perc)

print('--------------------------')
# ------------------------- End response question 4 -------------------------


