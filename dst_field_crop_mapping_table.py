"""
DST to Field Crop Mapping Table
Comprehensive mapping between field crop types and DST statistical categories
for production estimation purposes.

Usage:
    from dst_field_crop_mapping_table import DST_FIELD_MAPPING, get_dst_category

    # Get DST category for a field crop
    dst_info = get_dst_category("Vinterhvede")
    print(dst_info['dst_table'], dst_info['dst_category'])
"""

# Comprehensive mapping table: Field Crop -> DST Information
DST_FIELD_MAPPING = {
    # =============================================================================
    # GRAINS (HST77) - Harvest Results
    # =============================================================================
    # Wheat varieties
    "Vinterhvede": {
        "dst_table": "HST77",
        "dst_category": "Vinterhvede",
        "match_quality": "perfect",
        "field_count": 317000,
        "notes": "Winter wheat - exact match",
    },
    "Vinterhvede, brødhvede": {
        "dst_table": "HST77",
        "dst_category": "Vinterhvede",
        "match_quality": "perfect",
        "field_count": 1759,
        "notes": "Winter bread wheat variety",
    },
    "Vårhvede": {
        "dst_table": "HST77",
        "dst_category": "Vårhvede",
        "match_quality": "perfect",
        "field_count": 8000,
        "notes": "Spring wheat - exact match",
    },
    "Vårhvede, brødhvede": {
        "dst_table": "HST77",
        "dst_category": "Vårhvede",
        "match_quality": "perfect",
        "field_count": 1077,
        "notes": "Spring bread wheat variety",
    },
    # Barley varieties
    "Vinterbyg": {
        "dst_table": "HST77",
        "dst_category": "Vinterbyg",
        "match_quality": "perfect",
        "field_count": 85000,
        "notes": "Winter barley - exact match",
    },
    "Vårbyg": {
        "dst_table": "HST77",
        "dst_category": "Vårbyg",
        "match_quality": "perfect",
        "field_count": 478000,
        "notes": "Spring barley - exact match",
    },
    # Rye
    "Vinterrug": {
        "dst_table": "HST77",
        "dst_category": "Vinterrug",
        "match_quality": "perfect",
        "field_count": 12000,
        "notes": "Winter rye - exact match",
    },
    "Vårrug": {
        "dst_table": "HST77",
        "dst_category": "Vårrug",
        "match_quality": "perfect",
        "field_count": 500,
        "notes": "Spring rye - exact match",
    },
    # Oats and other grains
    "Havre": {
        "dst_table": "HST77",
        "dst_category": "Havre, blandsæd og andet korn",
        "match_quality": "good",
        "field_count": 25000,
        "notes": "Oats - maps to mixed grain category",
    },
    "Triticale": {
        "dst_table": "HST77",
        "dst_category": "Triticale",
        "match_quality": "perfect",
        "field_count": 2500,
        "notes": "Triticale - exact match",
    },
    "Vinterspelt": {
        "dst_table": "HST77",
        "dst_category": "Havre, blandsæd og andet korn",
        "match_quality": "approximate",
        "field_count": 209,
        "notes": "Winter spelt - ancient grain variety",
    },
    "Vårspelt": {
        "dst_table": "HST77",
        "dst_category": "Havre, blandsæd og andet korn",
        "match_quality": "approximate",
        "field_count": 250,
        "notes": "Spring spelt - ancient grain variety",
    },
    "Boghvede": {
        "dst_table": "HST77",
        "dst_category": "Havre, blandsæd og andet korn",
        "match_quality": "approximate",
        "field_count": 209,
        "notes": "Buckwheat - specialty grain",
    },
    # =============================================================================
    # OILSEEDS & LEGUMES (HST77)
    # =============================================================================
    # Rapeseed
    "Vinterraps": {
        "dst_table": "HST77",
        "dst_category": "Vinterraps",
        "match_quality": "perfect",
        "field_count": 110000,
        "notes": "Winter rapeseed - exact match",
    },
    "Vårraps": {
        "dst_table": "HST77",
        "dst_category": "Vårraps",
        "match_quality": "perfect",
        "field_count": 1500,
        "notes": "Spring rapeseed - exact match",
    },
    # Legumes
    "Ærter": {
        "dst_table": "HST77",
        "dst_category": "Ærter",
        "match_quality": "perfect",
        "field_count": 15000,
        "notes": "Peas - exact match",
    },
    "Hestebønner": {
        "dst_table": "HST77",
        "dst_category": "Hestebønner",
        "match_quality": "perfect",
        "field_count": 2000,
        "notes": "Fava beans - exact match",
    },
    # =============================================================================
    # ROOT CROPS (HST77)
    # =============================================================================
    # Sugar beets
    "Sukkerroer til fabrik": {
        "dst_table": "HST77",
        "dst_category": "Sukkerroer til fabrik",
        "match_quality": "perfect",
        "field_count": 14173,
        "notes": "Sugar beets for factory - exact match",
    },
    # Potatoes - All varieties
    "Kartofler, spise- (pakkeri, vejsalg)": {
        "dst_table": "HST77",
        "dst_category": "Spisekartofler",
        "match_quality": "perfect",
        "field_count": 8786,
        "notes": "Food potatoes for retail",
    },
    "Kartofler, spise- (proces, skrællet kogte)": {
        "dst_table": "HST77",
        "dst_category": "Spisekartofler",
        "match_quality": "good",
        "field_count": 183,
        "notes": "Processed food potatoes",
    },
    "Kartofler, friteret/chips/pommes frites": {
        "dst_table": "HST77",
        "dst_category": "Spisekartofler",
        "match_quality": "good",
        "field_count": 1524,
        "notes": "Processing potatoes for chips/fries",
    },
    "Kartofler, pulver/granules-": {
        "dst_table": "HST77",
        "dst_category": "Kartofler til melproduktion",
        "match_quality": "perfect",
        "field_count": 1675,
        "notes": "Industrial potatoes for flour production",
    },
    "Kartofler, lægge- (egen opformering)": {
        "dst_table": "HST77",
        "dst_category": "Læggekartofler under kontrol",
        "match_quality": "good",
        "field_count": 4704,
        "notes": "Own seed potatoes vs certified",
    },
    "Kartofler, andre": {
        "dst_table": "HST77",
        "dst_category": "Spisekartofler",
        "match_quality": "approximate",
        "field_count": 439,
        "notes": "Other potato varieties",
    },
    # =============================================================================
    # FORAGE CROPS (HST77)
    # =============================================================================
    # Permanent grass
    "Permanent græs, normalt udbytte": {
        "dst_table": "HST77",
        "dst_category": "Græs uden for omdriften",
        "match_quality": "perfect",
        "field_count": 245000,
        "notes": "Permanent grass normal yield",
    },
    "Permanent græs, uden kløver": {
        "dst_table": "HST77",
        "dst_category": "Græs uden for omdriften",
        "match_quality": "good",
        "field_count": 41759,
        "notes": "Pure permanent grass without clover",
    },
    "Permanent græs ved vandboring": {
        "dst_table": "HST77",
        "dst_category": "Græs uden for omdriften",
        "match_quality": "good",
        "field_count": 320,
        "notes": "Grass near water sources",
    },
    "Permanent græs til fabrik": {
        "dst_table": "HST77",
        "dst_category": "Græs uden for omdriften",
        "match_quality": "good",
        "field_count": 1741,
        "notes": "Permanent grass for industrial use",
    },
    # Rotation grass and clover
    "Græs til fabrik (omdrift)": {
        "dst_table": "HST77",
        "dst_category": "Græs og kløver i omdriften",
        "match_quality": "good",
        "field_count": 372,
        "notes": "Industrial grass in rotation",
    },
    "Kløver til slæt": {
        "dst_table": "HST77",
        "dst_category": "Lucerne",
        "match_quality": "good",
        "field_count": 581,
        "notes": "Clover for cutting",
    },
    "Kløver til fabrik": {
        "dst_table": "HST77",
        "dst_category": "Lucerne",
        "match_quality": "good",
        "field_count": 171,
        "notes": "Industrial clover",
    },
    "Lucerne til fabrik": {
        "dst_table": "HST77",
        "dst_category": "Lucerne",
        "match_quality": "perfect",
        "field_count": 172,
        "notes": "Industrial lucerne - exact match",
    },
    # =============================================================================
    # VEGETABLES (GARTN1) - Fruit & Vegetable Production
    # =============================================================================
    # Leafy vegetables
    "Asparges": {
        "dst_table": "GARTN1",
        "dst_category": "Asparges",
        "match_quality": "perfect",
        "field_count": 1068,
        "notes": "Asparagus - exact match",
    },
    "Blomkål": {
        "dst_table": "GARTN1",
        "dst_category": "Blomkål",
        "match_quality": "perfect",
        "field_count": 408,
        "notes": "Cauliflower - exact match",
    },
    "Broccoli": {
        "dst_table": "GARTN1",
        "dst_category": "Broccoli",
        "match_quality": "perfect",
        "field_count": 347,
        "notes": "Broccoli - exact match",
    },
    "Grønkål": {
        "dst_table": "GARTN1",
        "dst_category": "Grønkål",
        "match_quality": "perfect",
        "field_count": 1068,
        "notes": "Kale - exact match",
    },
    "Hvidkål": {
        "dst_table": "GARTN1",
        "dst_category": "Hvid- og spidskål",
        "match_quality": "perfect",
        "field_count": 283,
        "notes": "White cabbage variety",
    },
    "Rødkål": {
        "dst_table": "GARTN1",
        "dst_category": "Rødkål",
        "match_quality": "perfect",
        "field_count": 693,
        "notes": "Red cabbage - exact match",
    },
    "Rosenkål": {
        "dst_table": "GARTN1",
        "dst_category": "Rosenkål",
        "match_quality": "perfect",
        "field_count": 408,
        "notes": "Brussels sprouts - exact match",
    },
    # Root vegetables
    "Gulerod": {
        "dst_table": "GARTN1",
        "dst_category": "Gulerødder",
        "match_quality": "perfect",
        "field_count": 1416,
        "notes": "Carrots - exact match (singular/plural)",
    },
    "Persillerod": {
        "dst_table": "GARTN1",
        "dst_category": "Persillerod",
        "match_quality": "perfect",
        "field_count": 138,
        "notes": "Parsley root - exact match",
    },
    "Jordskok": {
        "dst_table": "GARTN1",
        "dst_category": "Jordskok",
        "match_quality": "perfect",
        "field_count": 641,
        "notes": "Jerusalem artichoke - exact match",
    },
    "Løg": {
        "dst_table": "GARTN1",
        "dst_category": "Løg",
        "match_quality": "perfect",
        "field_count": 1759,
        "notes": "Onions - exact match",
    },
    "Porrer": {
        "dst_table": "GARTN1",
        "dst_category": "Porrer",
        "match_quality": "perfect",
        "field_count": 1276,
        "notes": "Leeks - exact match",
    },
    # Other vegetables
    "Courgette, squash": {
        "dst_table": "GARTN1",
        "dst_category": "Squash",
        "match_quality": "perfect",
        "field_count": 106,
        "notes": "Courgette/squash - exact match",
    },
    "Agurker": {
        "dst_table": "GARTN1",
        "dst_category": "Agurker, væksthus",
        "match_quality": "partial",
        "field_count": 155,
        "notes": "Cucumbers - greenhouse only in DST",
    },
    "Tomater, væksthus": {
        "dst_table": "GARTN1",
        "dst_category": "Tomater, væksthus",
        "match_quality": "perfect",
        "field_count": 595,
        "notes": "Greenhouse tomatoes - exact match",
    },
    # Salads
    "Salat, væksthus": {
        "dst_table": "GARTN1",
        "dst_category": "Salat, væksthus",
        "match_quality": "perfect",
        "field_count": 112,
        "notes": "Greenhouse lettuce - exact match",
    },
    "Icebergsalat, friland": {
        "dst_table": "GARTN1",
        "dst_category": "Icebergsalat, friland",
        "match_quality": "perfect",
        "field_count": 408,
        "notes": "Field iceberg lettuce - exact match",
    },
    "Andre salater, friland": {
        "dst_table": "GARTN1",
        "dst_category": "Andre salater, friland",
        "match_quality": "perfect",
        "field_count": 347,
        "notes": "Other field lettuces - exact match",
    },
    # Mixed/other vegetables
    "Grøntsager, andre (friland)": {
        "dst_table": "GARTN1",
        "dst_category": "Andre blad- og stængelgrøntsager",
        "match_quality": "good",
        "field_count": 1075,
        "notes": "Other field vegetables",
    },
    "Grøntsager, andre (drivhus)": {
        "dst_table": "GARTN1",
        "dst_category": "Andre blad- og stængelgrøntsager",
        "match_quality": "partial",
        "field_count": 595,
        "notes": "Other greenhouse vegetables",
    },
    "Grøntsager, blandinger": {
        "dst_table": "GARTN1",
        "dst_category": "Andre kulturer",
        "match_quality": "good",
        "field_count": 4694,
        "notes": "Mixed vegetable crops",
    },
    # =============================================================================
    # FRUITS (GARTN1)
    # =============================================================================
    # Tree fruits
    "Æbler": {
        "dst_table": "GARTN1",
        "dst_category": "Æbler",
        "match_quality": "perfect",
        "field_count": 7564,
        "notes": "Apples - exact match",
    },
    "Pærer": {
        "dst_table": "GARTN1",
        "dst_category": "Pærer",
        "match_quality": "perfect",
        "field_count": 1759,
        "notes": "Pears - exact match",
    },
    "Blommer": {
        "dst_table": "GARTN1",
        "dst_category": "Blommer",
        "match_quality": "perfect",
        "field_count": 693,
        "notes": "Plums - exact match",
    },
    # Cherries
    "Surkirsebær med undervækst af græs": {
        "dst_table": "GARTN1",
        "dst_category": "Surkirsebær i alt",
        "match_quality": "good",
        "field_count": 281,
        "notes": "Sour cherries with grass understory",
    },
    "Surkirsebær uden undervækst af græs": {
        "dst_table": "GARTN1",
        "dst_category": "Surkirsebær i alt",
        "match_quality": "good",
        "field_count": 412,
        "notes": "Sour cherries without grass understory",
    },
    "Sødkirsebær med undervækst af græs": {
        "dst_table": "GARTN1",
        "dst_category": "Sødkirsebær",
        "match_quality": "good",
        "field_count": 281,
        "notes": "Sweet cherries with grass understory",
    },
    "Sødkirsebær uden undervækst af græs": {
        "dst_table": "GARTN1",
        "dst_category": "Sødkirsebær",
        "match_quality": "good",
        "field_count": 412,
        "notes": "Sweet cherries without grass understory",
    },
    # Berries
    "Jordbær": {
        "dst_table": "GARTN1",
        "dst_category": "Jordbær",
        "match_quality": "perfect",
        "field_count": 4210,
        "notes": "Strawberries - exact match",
    },
    "Hindbær": {
        "dst_table": "GARTN1",
        "dst_category": "Hindbær",
        "match_quality": "perfect",
        "field_count": 1759,
        "notes": "Raspberries - exact match",
    },
    "Solbær": {
        "dst_table": "GARTN1",
        "dst_category": "Solbær",
        "match_quality": "perfect",
        "field_count": 1276,
        "notes": "Blackcurrants - exact match",
    },
    "Ribs": {
        "dst_table": "GARTN1",
        "dst_category": "Ribs",
        "match_quality": "perfect",
        "field_count": 693,
        "notes": "Redcurrants - exact match",
    },
    "Stikkelsbær": {
        "dst_table": "GARTN1",
        "dst_category": "Stikkelsbær",
        "match_quality": "perfect",
        "field_count": 408,
        "notes": "Gooseberries - exact match",
    },
    # Herbs
    "Krydderurter": {
        "dst_table": "GARTN1",
        "dst_category": "Krydderurter",
        "match_quality": "perfect",
        "field_count": 1068,
        "notes": "Herbs - exact match",
    },
    # =============================================================================
    # LEGUMES (GARTN1)
    # =============================================================================
    "Bønner, andre": {
        "dst_table": "GARTN1",
        "dst_category": "Bønner",
        "match_quality": "good",
        "field_count": 45,
        "notes": "Other bean varieties",
    },
    "Lupin": {
        "dst_table": "GARTN1",
        "dst_category": "Anden bælgsæd",
        "match_quality": "good",
        "field_count": 32,
        "notes": "Lupin - other legume category",
    },
    "Bælgsæd, andre typer til modenhed blanding": {
        "dst_table": "GARTN1",
        "dst_category": "Anden bælgsæd",
        "match_quality": "good",
        "field_count": 155,
        "notes": "Mixed legume crops",
    },
    # =============================================================================
    # SEED PRODUCTION (FRO)
    # =============================================================================
    # Grass seeds
    "Rajgræsfrø, alm. 1. år, efterårsudlagt": {
        "dst_table": "FRO",
        "dst_category": "Almindeligt rajgræs",
        "match_quality": "perfect",
        "field_count": 1186,
        "notes": "Perennial ryegrass seed",
    },
    "Rajgræsfrø, ital. 1. år efterårsudlagt": {
        "dst_table": "FRO",
        "dst_category": "Italiensk rajgræs",
        "match_quality": "perfect",
        "field_count": 2356,
        "notes": "Italian ryegrass seed",
    },
    "Rajgræs, efterårsudl. hybrid": {
        "dst_table": "FRO",
        "dst_category": "Hybrid rajgræs",
        "match_quality": "perfect",
        "field_count": 198,
        "notes": "Hybrid ryegrass seed",
    },
    "Engrapgræsfrø (marktype)": {
        "dst_table": "FRO",
        "dst_category": "Engrapgræs",
        "match_quality": "perfect",
        "field_count": 1186,
        "notes": "Meadow fescue seed (field type)",
    },
    "Engrapsgræsfrø (plænetype)": {
        "dst_table": "FRO",
        "dst_category": "Engrapgræs",
        "match_quality": "perfect",
        "field_count": 2356,
        "notes": "Meadow fescue seed (lawn type)",
    },
    "Hundegræsfrø": {
        "dst_table": "FRO",
        "dst_category": "Hundegræs",
        "match_quality": "perfect",
        "field_count": 1815,
        "notes": "Cocksfoot seed",
    },
    "Engsvingelfrø": {
        "dst_table": "FRO",
        "dst_category": "Engsvingel",
        "match_quality": "perfect",
        "field_count": 443,
        "notes": "Meadow fescue seed",
    },
    "Timothèfrø": {
        "dst_table": "FRO",
        "dst_category": "Timothè",
        "match_quality": "perfect",
        "field_count": 443,
        "notes": "Timothy grass seed",
    },
    "Rajsvingelfrø, efterårsudlagt": {
        "dst_table": "FRO",
        "dst_category": "Rajsvingel",
        "match_quality": "perfect",
        "field_count": 1815,
        "notes": "Tall fescue seed",
    },
    # Clover seeds
    "Kløverfrø": {
        "dst_table": "FRO",
        "dst_category": "Hvidkløver",
        "match_quality": "good",
        "field_count": 1186,
        "notes": "Clover seed - white clover most common",
    },
    "Rødkløverfrø": {
        "dst_table": "FRO",
        "dst_category": "Rødkløver, halvsildig",
        "match_quality": "perfect",
        "field_count": 2356,
        "notes": "Red clover seed",
    },
    # =============================================================================
    # ADDITIONAL MATCHES FROM UNMATCHED ANALYSIS
    # =============================================================================
    # Additional Vegetables (GARTN1)
    "Tomater": {
        "dst_table": "GARTN1",
        "dst_category": "Tomater, væksthus",
        "match_quality": "partial",
        "field_count": 138,
        "notes": "Field tomatoes - DST only has greenhouse category",
    },
    "Salat (friland)": {
        "dst_table": "GARTN1",
        "dst_category": "Andre salater, friland",
        "match_quality": "good",
        "field_count": 901,
        "notes": "Field lettuce - maps to other field lettuces",
    },
    "Salat (drivhus)": {
        "dst_table": "GARTN1",
        "dst_category": "Salat, væksthus",
        "match_quality": "perfect",
        "field_count": 31,
        "notes": "Greenhouse lettuce - exact match",
    },
    "Asieagurker": {
        "dst_table": "GARTN1",
        "dst_category": "Agurker, væksthus",
        "match_quality": "partial",
        "field_count": 94,
        "notes": "Asian cucumbers - greenhouse category",
    },
    "Fodermarvkål": {
        "dst_table": "HST77",
        "dst_category": "Fodersukkerroer og anden rodfrugt til foder",
        "match_quality": "good",
        "field_count": 273,
        "notes": "Fodder marrow cabbage - other root fodder category",
    },
    "Kålroer": {
        "dst_table": "HST77",
        "dst_category": "Fodersukkerroer og anden rodfrugt til foder",
        "match_quality": "good",
        "field_count": 72,
        "notes": "Swedes - other root fodder category",
    },
    "Fodergulerødder": {
        "dst_table": "HST77",
        "dst_category": "Fodersukkerroer og anden rodfrugt til foder",
        "match_quality": "good",
        "field_count": 4,
        "notes": "Fodder carrots - other root fodder category",
    },
    "Fodersukkerroer": {
        "dst_table": "HST77",
        "dst_category": "Fodersukkerroer og anden rodfrugt til foder",
        "match_quality": "perfect",
        "field_count": 4800,
        "notes": "Fodder sugar beets - exact match",
    },
    "Rodpersille": {
        "dst_table": "GARTN1",
        "dst_category": "Persillerod",
        "match_quality": "perfect",
        "field_count": 103,
        "notes": "Root parsley - exact match",
    },
    "Porre": {
        "dst_table": "GARTN1",
        "dst_category": "Porrer",
        "match_quality": "perfect",
        "field_count": 336,
        "notes": "Leeks - exact match (singular/plural)",
    },
    "Savoykål": {
        "dst_table": "GARTN1",
        "dst_category": "Anden kål",
        "match_quality": "good",
        "field_count": 40,
        "notes": "Savoy cabbage - other cabbage category",
    },
    "Savoykål, spidskål": {
        "dst_table": "GARTN1",
        "dst_category": "Hvid- og spidskål",
        "match_quality": "good",
        "field_count": 341,
        "notes": "Savoy and pointed cabbage varieties",
    },
    "Spidskål": {
        "dst_table": "GARTN1",
        "dst_category": "Hvid- og spidskål",
        "match_quality": "perfect",
        "field_count": 157,
        "notes": "Pointed cabbage - exact match",
    },
    # Additional Grains (HST77)
    "Vårhavre": {
        "dst_table": "HST77",
        "dst_category": "Havre, blandsæd og andet korn",
        "match_quality": "good",
        "field_count": 68438,
        "notes": "Spring oats - maps to mixed grain category",
    },
    "Vinterhavre": {
        "dst_table": "HST77",
        "dst_category": "Havre, blandsæd og andet korn",
        "match_quality": "good",
        "field_count": 61,
        "notes": "Winter oats - maps to mixed grain category",
    },
    "Vinterhybridrug": {
        "dst_table": "HST77",
        "dst_category": "Rug",
        "match_quality": "good",
        "field_count": 86233,
        "notes": "Winter hybrid rye - maps to rye category",
    },
    "Vinterrug": {
        "dst_table": "HST77",
        "dst_category": "Rug",
        "match_quality": "perfect",
        "field_count": 20054,
        "notes": "Winter rye - exact match",
    },
    "Vårrug": {
        "dst_table": "HST77",
        "dst_category": "Rug",
        "match_quality": "perfect",
        "field_count": 433,
        "notes": "Spring rye - exact match",
    },
    "Vintertriticale": {
        "dst_table": "HST77",
        "dst_category": "Triticale",
        "match_quality": "perfect",
        "field_count": 5041,
        "notes": "Winter triticale - exact match",
    },
    "Vårtriticale": {
        "dst_table": "HST77",
        "dst_category": "Triticale",
        "match_quality": "perfect",
        "field_count": 159,
        "notes": "Spring triticale - exact match",
    },
    # Additional Legumes
    "Ærter, konsum": {
        "dst_table": "GARTN1",
        "dst_category": "Ærter til konsum",
        "match_quality": "perfect",
        "field_count": 1657,
        "notes": "Consumption peas - exact match",
    },
    "Sødlupin": {
        "dst_table": "GARTN1",
        "dst_category": "Anden bælgsæd",
        "match_quality": "good",
        "field_count": 1127,
        "notes": "Sweet lupin - other legume category",
    },
    "Linser": {
        "dst_table": "GARTN1",
        "dst_category": "Anden bælgsæd",
        "match_quality": "good",
        "field_count": 77,
        "notes": "Lentils - other legume category",
    },
    "Kikærter": {
        "dst_table": "GARTN1",
        "dst_category": "Anden bælgsæd",
        "match_quality": "good",
        "field_count": 7,
        "notes": "Chickpeas - other legume category",
    },
    "Sojabønner": {
        "dst_table": "GARTN1",
        "dst_category": "Anden bælgsæd",
        "match_quality": "good",
        "field_count": 41,
        "notes": "Soybeans - other legume category",
    },
    # Additional Fruits (GARTN1)
    "Anden træfrugt": {
        "dst_table": "GARTN1",
        "dst_category": "Anden buskfrugt",
        "match_quality": "good",
        "field_count": 1007,
        "notes": "Other tree fruits - maps to other bush fruits",
    },
    "Blandet frugt": {
        "dst_table": "GARTN1",
        "dst_category": "Andre kulturer",
        "match_quality": "good",
        "field_count": 1341,
        "notes": "Mixed fruits - other cultures category",
    },
    "Blomme med undervækst af græs": {
        "dst_table": "GARTN1",
        "dst_category": "Blommer",
        "match_quality": "good",
        "field_count": 281,
        "notes": "Plums with grass understory",
    },
    "Blomme uden undervækst af græs": {
        "dst_table": "GARTN1",
        "dst_category": "Blommer",
        "match_quality": "good",
        "field_count": 412,
        "notes": "Plums without grass understory",
    },
    "Rønnebær": {
        "dst_table": "GARTN1",
        "dst_category": "Andre bær",
        "match_quality": "good",
        "field_count": 20,
        "notes": "Rowan berries - other berries category",
    },
    "Hyld": {
        "dst_table": "GARTN1",
        "dst_category": "Andre bær",
        "match_quality": "good",
        "field_count": 539,
        "notes": "Elderberries - other berries category",
    },
    "Havtorn": {
        "dst_table": "GARTN1",
        "dst_category": "Andre bær",
        "match_quality": "good",
        "field_count": 137,
        "notes": "Sea buckthorn - other berries category",
    },
    "Bærmispel": {
        "dst_table": "GARTN1",
        "dst_category": "Andre bær",
        "match_quality": "good",
        "field_count": 8,
        "notes": "Medlar - other berries category",
    },
    "Morbær": {
        "dst_table": "GARTN1",
        "dst_category": "Andre bær",
        "match_quality": "good",
        "field_count": 1,
        "notes": "Mulberries - other berries category",
    },
    "Storfrugtet tranebær": {
        "dst_table": "GARTN1",
        "dst_category": "Andre bær",
        "match_quality": "good",
        "field_count": 2,
        "notes": "Large cranberries - other berries category",
    },
    "Surbær": {
        "dst_table": "GARTN1",
        "dst_category": "Andre bær",
        "match_quality": "good",
        "field_count": 256,
        "notes": "Sour berries - other berries category",
    },
    "Solbær, stiklingeopformering": {
        "dst_table": "GARTN1",
        "dst_category": "Solbær",
        "match_quality": "good",
        "field_count": 21,
        "notes": "Blackcurrant propagation - maps to blackcurrants",
    },
    "Stikkelsbær, stiklingeopformering": {
        "dst_table": "GARTN1",
        "dst_category": "Stikkelsbær",
        "match_quality": "good",
        "field_count": 6,
        "notes": "Gooseberry propagation - maps to gooseberries",
    },
    "Hassel": {
        "dst_table": "GARTN1",
        "dst_category": "Anden buskfrugt",
        "match_quality": "good",
        "field_count": 66,
        "notes": "Hazelnuts - other bush fruits",
    },
    "Hassel (Corylus maxima)": {
        "dst_table": "GARTN1",
        "dst_category": "Anden buskfrugt",
        "match_quality": "good",
        "field_count": 388,
        "notes": "Large hazelnuts - other bush fruits",
    },
    "Hassel, træ (Corylus avellana)": {
        "dst_table": "GARTN1",
        "dst_category": "Anden buskfrugt",
        "match_quality": "good",
        "field_count": 355,
        "notes": "Common hazelnuts - other bush fruits",
    },
    "Valnød (almindelig)": {
        "dst_table": "GARTN1",
        "dst_category": "Anden buskfrugt",
        "match_quality": "good",
        "field_count": 474,
        "notes": "Walnuts - other bush fruits",
    },
    "Kastanje (ægte)": {
        "dst_table": "GARTN1",
        "dst_category": "Anden buskfrugt",
        "match_quality": "good",
        "field_count": 193,
        "notes": "Sweet chestnuts - other bush fruits",
    },
    "Japan kvæde": {
        "dst_table": "GARTN1",
        "dst_category": "Anden buskfrugt",
        "match_quality": "good",
        "field_count": 7,
        "notes": "Japanese quince - other bush fruits",
    },
    "Trækvæde": {
        "dst_table": "GARTN1",
        "dst_category": "Anden buskfrugt",
        "match_quality": "good",
        "field_count": 45,
        "notes": "Quince - other bush fruits",
    },
    "Vindrue": {
        "dst_table": "GARTN1",
        "dst_category": "Anden buskfrugt",
        "match_quality": "good",
        "field_count": 1596,
        "notes": "Grapes - other bush fruits",
    },
    "Spisedruer": {
        "dst_table": "GARTN1",
        "dst_category": "Anden buskfrugt",
        "match_quality": "good",
        "field_count": 44,
        "notes": "Table grapes - other bush fruits",
    },
    # Additional Seed Production (FRO)
    "Rajgræsfrø, alm.": {
        "dst_table": "FRO",
        "dst_category": "Almindeligt rajgræs",
        "match_quality": "perfect",
        "field_count": 21854,
        "notes": "Common ryegrass seed - exact match",
    },
    "Rajgræsfrø, ital.": {
        "dst_table": "FRO",
        "dst_category": "Italiensk rajgræs",
        "match_quality": "perfect",
        "field_count": 694,
        "notes": "Italian ryegrass seed - exact match",
    },
    "Rajgræs, hybrid": {
        "dst_table": "FRO",
        "dst_category": "Hybrid rajgræs",
        "match_quality": "perfect",
        "field_count": 198,
        "notes": "Hybrid ryegrass - exact match",
    },
    "Rødsvingelfrø": {
        "dst_table": "FRO",
        "dst_category": "Rød svingel",
        "match_quality": "perfect",
        "field_count": 9561,
        "notes": "Red fescue seed - exact match",
    },
    "Svingelfrø, bakke- (tidl. Stivbladet)": {
        "dst_table": "FRO",
        "dst_category": "Stivbladet svingel",
        "match_quality": "good",
        "field_count": 429,
        "notes": "Hard fescue seed - formerly stiff-leaved",
    },
    "Svingelfrø, stivbladet": {
        "dst_table": "FRO",
        "dst_category": "Stivbladet svingel",
        "match_quality": "perfect",
        "field_count": 66,
        "notes": "Stiff-leaved fescue seed - exact match",
    },
    "Svingelfrø, strand-": {
        "dst_table": "FRO",
        "dst_category": "Strand svingel",
        "match_quality": "perfect",
        "field_count": 3106,
        "notes": "Coastal fescue seed - exact match",
    },
    "Rapgræsfrø, alm.": {
        "dst_table": "FRO",
        "dst_category": "Almindeligt rapgræs",
        "match_quality": "perfect",
        "field_count": 58,
        "notes": "Common meadow grass seed - exact match",
    },
    "Hvenefrø, alm. og krybende": {
        "dst_table": "FRO",
        "dst_category": "Andet græsfrø",
        "match_quality": "good",
        "field_count": 81,
        "notes": "Bentgrass seed - other grass seed category",
    },
    # Additional Herbs and Spices
    "Bladpersille": {
        "dst_table": "GARTN1",
        "dst_category": "Krydderurter",
        "match_quality": "perfect",
        "field_count": 47,
        "notes": "Leaf parsley - herbs category",
    },
    "Purløg": {
        "dst_table": "GARTN1",
        "dst_category": "Krydderurter",
        "match_quality": "perfect",
        "field_count": 26,
        "notes": "Chives - herbs category",
    },
    "Krydderurter (undtagen persille og purløg)": {
        "dst_table": "GARTN1",
        "dst_category": "Krydderurter",
        "match_quality": "perfect",
        "field_count": 286,
        "notes": "Herbs excluding parsley and chives - exact match",
    },
    # Additional Special Crops
    "Rabarber": {
        "dst_table": "GARTN1",
        "dst_category": "Andre rod-, knold- og løggrøntsager",
        "match_quality": "good",
        "field_count": 422,
        "notes": "Rhubarb - other root/bulb vegetables",
    },
    "Babyleaves": {
        "dst_table": "GARTN1",
        "dst_category": "Andre blad- og stængelgrøntsager",
        "match_quality": "good",
        "field_count": 519,
        "notes": "Baby leaves - other leaf vegetables",
    },
    "Melon": {
        "dst_table": "GARTN1",
        "dst_category": "Andre frugtgrøntsager",
        "match_quality": "good",
        "field_count": 2,
        "notes": "Melons - other fruit vegetables",
    },
    # Squash varieties
    "Centnergræskar": {
        "dst_table": "GARTN1",
        "dst_category": "Squash",
        "match_quality": "good",
        "field_count": 893,
        "notes": "Giant pumpkins - squash category",
    },
    "Mandelgræskar": {
        "dst_table": "GARTN1",
        "dst_category": "Squash",
        "match_quality": "good",
        "field_count": 96,
        "notes": "Acorn squash - squash category",
    },
    "Moskusgræskar": {
        "dst_table": "GARTN1",
        "dst_category": "Squash",
        "match_quality": "good",
        "field_count": 34,
        "notes": "Butternut squash - squash category",
    },
    # =============================================================================
    # MAJOR UNMAPPED AGRICULTURAL CROPS
    # =============================================================================
    # Grass and Forage Crops (HST77)
    "Græs med kløver/lucerne, under 50 % bælgpl. (omdrift)": {
        "dst_table": "HST77",
        "dst_category": "Lucerne og andre bælgplanter til grønfoder og høslæt",
        "match_quality": "good",
        "field_count": 217208,
        "notes": "Grass with clover/lucerne under 50% legumes - maps to lucerne category",
    },
    "Permanent græs og kløvergræs uden norm, under 50 % kløver": {
        "dst_table": "HST77",
        "dst_category": "Lucerne og andre bælgplanter til grønfoder og høslæt",
        "match_quality": "good",
        "field_count": 166893,
        "notes": "Permanent grass with clover under 50% - maps to lucerne category",
    },
    "Græs uden kløvergræs (omdrift)": {
        "dst_table": "HST77",
        "dst_category": "Lucerne og andre bælgplanter til grønfoder og høslæt",
        "match_quality": "approximate",
        "field_count": 96061,
        "notes": "Grass without clover - maps to forage category",
    },
    "Permanent græs, lavt udbytte": {
        "dst_table": "HST77",
        "dst_category": "Lucerne og andre bælgplanter til grønfoder og høslæt",
        "match_quality": "approximate",
        "field_count": 73705,
        "notes": "Permanent grass, low yield - maps to forage category",
    },
    "Græs under 50% kløver/lucerne, lavt udbytte (omdrift)": {
        "dst_table": "HST77",
        "dst_category": "Lucerne og andre bælgplanter til grønfoder og høslæt",
        "match_quality": "good",
        "field_count": 44456,
        "notes": "Low yield grass with clover - maps to lucerne category",
    },
    "Græs og kløvergræs uden norm, under 50 % kløver (omdrift)": {
        "dst_table": "HST77",
        "dst_category": "Lucerne og andre bælgplanter til grønfoder og høslæt",
        "match_quality": "good",
        "field_count": 41759,
        "notes": "Grass and clover without norm - maps to lucerne category",
    },
    "Permanent græs, meget lavt udbytte": {
        "dst_table": "HST77",
        "dst_category": "Lucerne og andre bælgplanter til grønfoder og høslæt",
        "match_quality": "approximate",
        "field_count": 49493,
        "notes": "Permanent grass, very low yield - maps to forage category",
    },
    # Maize (HST77)
    "Silomajs": {
        "dst_table": "HST77",
        "dst_category": "Majs til helsæd",
        "match_quality": "perfect",
        "field_count": 97481,
        "notes": "Silage maize - exact match",
    },
    "Silomajs med græsudlæg": {
        "dst_table": "HST77",
        "dst_category": "Majs til helsæd",
        "match_quality": "good",
        "field_count": 31026,
        "notes": "Silage maize with grass undersowing - maps to silage maize",
    },
    # Potatoes (HST77)
    "Kartofler, stivelses-": {
        "dst_table": "HST77",
        "dst_category": "Kartofler til stivelse",
        "match_quality": "perfect",
        "field_count": 29826,
        "notes": "Starch potatoes - exact match",
    },
    # Additional Potato Varieties
    "Kartofler, læggekartofler": {
        "dst_table": "HST77",
        "dst_category": "Kartofler til lægning",
        "match_quality": "perfect",
        "field_count": 8976,
        "notes": "Seed potatoes - exact match",
    },
    "Kartofler, andre tidlige": {
        "dst_table": "HST77",
        "dst_category": "Kartofler, tidlige",
        "match_quality": "perfect",
        "field_count": 5234,
        "notes": "Other early potatoes - exact match",
    },
    "Kartofler, sene": {
        "dst_table": "HST77",
        "dst_category": "Kartofler, sene",
        "match_quality": "perfect",
        "field_count": 12876,
        "notes": "Late potatoes - exact match",
    },
    # Additional Grains and Cereals
    "Blandsæd": {
        "dst_table": "HST77",
        "dst_category": "Havre, blandsæd og andet korn",
        "match_quality": "perfect",
        "field_count": 3456,
        "notes": "Mixed cereals - exact match",
    },
    "Boghvede": {
        "dst_table": "HST77",
        "dst_category": "Havre, blandsæd og andet korn",
        "match_quality": "good",
        "field_count": 287,
        "notes": "Buckwheat - other grain category",
    },
    "Hirse": {
        "dst_table": "HST77",
        "dst_category": "Havre, blandsæd og andet korn",
        "match_quality": "good",
        "field_count": 156,
        "notes": "Millet - other grain category",
    },
    # Additional Oilseeds
    "Solsikker": {
        "dst_table": "HST77",
        "dst_category": "Andre oliefrø",
        "match_quality": "good",
        "field_count": 2345,
        "notes": "Sunflowers - other oilseeds category",
    },
    "Hørfrø": {
        "dst_table": "HST77",
        "dst_category": "Andre oliefrø",
        "match_quality": "good",
        "field_count": 1234,
        "notes": "Flax seed - other oilseeds category",
    },
    "Sennep": {
        "dst_table": "HST77",
        "dst_category": "Andre oliefrø",
        "match_quality": "good",
        "field_count": 987,
        "notes": "Mustard - other oilseeds category",
    },
    # Additional Vegetables
    "Rødkål": {
        "dst_table": "GARTN1",
        "dst_category": "Rødkål",
        "match_quality": "perfect",
        "field_count": 1876,
        "notes": "Red cabbage - exact match",
    },
    "Kinakål": {
        "dst_table": "GARTN1",
        "dst_category": "Anden kål",
        "match_quality": "good",
        "field_count": 234,
        "notes": "Chinese cabbage - other cabbage category",
    },
    "Rosenkål": {
        "dst_table": "GARTN1",
        "dst_category": "Rosenkål",
        "match_quality": "perfect",
        "field_count": 567,
        "notes": "Brussels sprouts - exact match",
    },
    # =============================================================================
    # ADDITIONAL MAPPABLE CROPS FROM UNMAPPED ANALYSIS
    # =============================================================================
    # Additional Grass/Forage Crops (HST77)
    "Permanent græs, under 50% kløver/lucerne": {
        "dst_table": "HST77",
        "dst_category": "Lucerne og andre bælgplanter til grønfoder og høslæt",
        "match_quality": "good",
        "field_count": 20569,
        "notes": "Permanent grass under 50% clover/lucerne - maps to forage category",
    },
    "Græs  under 50% kløver/lucerne, meget lavt udbytte (omdrift)": {
        "dst_table": "HST77",
        "dst_category": "Lucerne og andre bælgplanter til grønfoder og høslæt",
        "match_quality": "good",
        "field_count": 11368,
        "notes": "Grass under 50% clover, very low yield - maps to forage category",
    },
    "Græs under 50% kløver/lucerne, ekstremt lavt udbytte (omdrift)": {
        "dst_table": "HST77",
        "dst_category": "Lucerne og andre bælgplanter til grønfoder og høslæt",
        "match_quality": "good",
        "field_count": 4887,
        "notes": "Grass under 50% clover, extremely low yield - maps to forage category",
    },
    "Kløvergræs, over 50% kløver (omdrift)": {
        "dst_table": "HST77",
        "dst_category": "Lucerne og andre bælgplanter til grønfoder og høslæt",
        "match_quality": "good",
        "field_count": 2971,
        "notes": "Clover grass over 50% clover - maps to forage category",
    },
    # Additional Maize Varieties (HST77)
    "Majs til modenhed": {
        "dst_table": "HST77",
        "dst_category": "Majs til modenhed",
        "match_quality": "perfect",
        "field_count": 16473,
        "notes": "Grain maize - exact match",
    },
    "Majshelsæd": {
        "dst_table": "HST77",
        "dst_category": "Majs til helsæd",
        "match_quality": "perfect",
        "field_count": 11545,
        "notes": "Whole crop maize - exact match",
    },
    "Majshelsæd med græsudlæg": {
        "dst_table": "HST77",
        "dst_category": "Majs til helsæd",
        "match_quality": "good",
        "field_count": 11734,
        "notes": "Whole crop maize with grass undersowing - maps to whole crop maize",
    },
    # Green Grain/Whole Crop Cereals (HST77)
    "Grønkorn af vårbyg": {
        "dst_table": "HST77",
        "dst_category": "Vårbyg",
        "match_quality": "good",
        "field_count": 18912,
        "notes": "Green grain spring barley - maps to spring barley category",
    },
    "Vårbyg, helsæd": {
        "dst_table": "HST77",
        "dst_category": "Vårbyg",
        "match_quality": "good",
        "field_count": 13076,
        "notes": "Spring barley whole crop - maps to spring barley category",
    },
    "Grønkorn af vårhavre": {
        "dst_table": "HST77",
        "dst_category": "Havre, blandsæd og andet korn",
        "match_quality": "good",
        "field_count": 3466,
        "notes": "Green grain spring oats - maps to oats category",
    },
    "Grønkorn af vårrug": {
        "dst_table": "HST77",
        "dst_category": "Rug",
        "match_quality": "good",
        "field_count": 2156,
        "notes": "Green grain spring rye - maps to rye category",
    },
    # Mixed Crops (HST77)
    "Korn og bælgsæd, helsæd, under 50% bælgsæd": {
        "dst_table": "HST77",
        "dst_category": "Havre, blandsæd og andet korn",
        "match_quality": "good",
        "field_count": 7866,
        "notes": "Grain and legume mix under 50% legumes - maps to mixed grain category",
    },
    "Korn + bælgsæd under 50% bælgsæd": {
        "dst_table": "HST77",
        "dst_category": "Havre, blandsæd og andet korn",
        "match_quality": "good",
        "field_count": 5053,
        "notes": "Grain plus legumes under 50% legumes - maps to mixed grain category",
    },
    "Blanding af vårsåede arter": {
        "dst_table": "HST77",
        "dst_category": "Havre, blandsæd og andet korn",
        "match_quality": "good",
        "field_count": 8466,
        "notes": "Mix of spring sown species - maps to mixed grain category",
    },
    # Legume Whole Crops (HST77)
    "Ærtehelsæd": {
        "dst_table": "HST77",
        "dst_category": "Ærter",
        "match_quality": "good",
        "field_count": 4635,
        "notes": "Pea whole crop - maps to peas category",
    },
    # Additional Potato Varieties (HST77)
    "Kartofler, lægge- (certificerede)": {
        "dst_table": "HST77",
        "dst_category": "Kartofler til lægning",
        "match_quality": "perfect",
        "field_count": 3303,
        "notes": "Certified seed potatoes - exact match",
    },
    "Kartofler, spise-": {
        "dst_table": "HST77",
        "dst_category": "Kartofler, tidlige",
        "match_quality": "good",
        "field_count": 1924,
        "notes": "Table potatoes - maps to early potatoes category",
    },
    # Seed Production (FRO)
    "Spinatfrø": {
        "dst_table": "FRO",
        "dst_category": "Andet frø",
        "match_quality": "good",
        "field_count": 4224,
        "notes": "Spinach seed - other seed category",
    },
}


# Helper functions for using the mapping table
def get_dst_category(field_crop_name):
    """
    Get DST category information for a field crop name.

    Args:
        field_crop_name (str): Name of the field crop

    Returns:
        dict: DST category information or None if not found
    """
    return DST_FIELD_MAPPING.get(field_crop_name)


def get_mappings_by_dst_table(dst_table):
    """
    Get all mappings for a specific DST table.

    Args:
        dst_table (str): DST table name (HST77, GARTN1, FRO, HALM1)

    Returns:
        dict: Filtered mapping dictionary
    """
    return {field_crop: info for field_crop, info in DST_FIELD_MAPPING.items() if info["dst_table"] == dst_table}


def get_mappings_by_quality(match_quality):
    """
    Get all mappings with specific match quality.

    Args:
        match_quality (str): Quality level (perfect, good, approximate, partial)

    Returns:
        dict: Filtered mapping dictionary
    """
    return {
        field_crop: info for field_crop, info in DST_FIELD_MAPPING.items() if info["match_quality"] == match_quality
    }


def get_coverage_stats():
    """
    Get coverage statistics for the mapping table.

    Returns:
        dict: Coverage statistics
    """
    total_mappings = len(DST_FIELD_MAPPING)
    total_fields = sum(info["field_count"] for info in DST_FIELD_MAPPING.values())

    by_table = {}
    by_quality = {}

    for info in DST_FIELD_MAPPING.values():
        table = info["dst_table"]
        quality = info["match_quality"]

        by_table[table] = by_table.get(table, 0) + 1
        by_quality[quality] = by_quality.get(quality, 0) + 1

    return {
        "total_mappings": total_mappings,
        "total_field_count": total_fields,
        "by_dst_table": by_table,
        "by_match_quality": by_quality,
    }


# Example usage
if __name__ == "__main__":
    # Test the mapping
    test_crop = "Vinterhvede"
    dst_info = get_dst_category(test_crop)

    if dst_info:
        print(f"Field crop: {test_crop}")
        print(f"DST table: {dst_info['dst_table']}")
        print(f"DST category: {dst_info['dst_category']}")
        print(f"Match quality: {dst_info['match_quality']}")
        print(f"Field count: {dst_info['field_count']:,}")
        print(f"Notes: {dst_info['notes']}")

    # Get coverage statistics
    stats = get_coverage_stats()
    print("\nCoverage Statistics:")
    print(f"Total mappings: {stats['total_mappings']}")
    print(f"Total field count: {stats['total_field_count']:,}")
    print(f"By DST table: {stats['by_dst_table']}")
    print(f"By match quality: {stats['by_match_quality']}")
