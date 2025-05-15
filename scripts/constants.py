filtered_columns = [
    # General Data
    "NU_ANO_CENSO",         # Census year
    "CO_ENTIDADE",          # Unique school ID
    "NO_ENTIDADE",          # School name
    "CO_UF",                # State code
    "SG_UF",                # State abbreviation
    "CO_MUNICIPIO",         # Municipality code
    "NO_MUNICIPIO",         # Municipality name
    "TP_DEPENDENCIA",       # Type of administrative dependency
    "TP_LOCALIZACAO",       # Indicates if school is located in urban or rural area
    "TP_SITUACAO_FUNCIONAMENTO",  # Indicates whether the school is operating
    
     # School infrastructure
    "IN_AGUA_POTAVEL",                 # Drinking water
    "IN_ENERGIA_REDE_PUBLICA",        # Public electric power
    "IN_ESGOTO_REDE_PUBLICA",         # Public sanitary sewer system
    "IN_LIXO_SERVICO_COLETA",         # Garbage collection service
    "IN_TRATAMENTO_LIXO_SEPARACAO",   # Waste separation
    "IN_TRATAMENTO_LIXO_REUTILIZA",   # Waste reuse
    "IN_TRATAMENTO_LIXO_RECICLAGEM",  # Waste recycling

    "IN_BANHEIRO",                    # School has restrooms
    "IN_BANHEIRO_PNE",                # Accessible restroom for people with disabilities
    "IN_REFEITORIO",                  # School has cafeteria
    "IN_ALIMENTACAO",                 # School provides meals
    "IN_QUADRA_ESPORTES",            # School has sports court
    "IN_QUADRA_ESPORTES_COBERTA",    # Covered sports court
    "IN_BIBLIOTECA",                 # Library available
    "IN_LABORATORIO_CIENCIAS",       # Science lab available
    "IN_LABORATORIO_INFORMATICA",    # Computer lab available

    # Pandemic-related and remote learning resources
    "IN_MEDIACAO_EAD",               # Remote learning
    "IN_MEDIACAO_SEMIPRESENCIAL",    # Hybrid learning
    "IN_MEDIACAO_PRESENCIAL",        # In-person learning

    # Technology and connectivity
    "IN_INTERNET",                   # Internet access available
    "IN_COMPUTADOR",                 # Computer access available
    "IN_DESKTOP_ALUNO",              # Desktop computers available for students
    "IN_COMP_PORTATIL_ALUNO",        # Laptops available for students
    "QT_DESKTOP_ALUNO",              # Number of desktop computers for students
    "QT_COMP_PORTATIL_ALUNO",        # Number of laptops for students
    "IN_EQUIP_MULTIMIDIA",           # Multimedia equipment available
    "IN_EQUIP_TV",                   # TV available

    # Support professionals
    "IN_PROF_PSICOLOGO",             # Psychologist available
    "QT_PROF_PSICOLOGO",             # Number of psychologists
    "IN_PROF_ASSIST_SOCIAL",         # Social worker available
    "QT_PROF_ASSIST_SOCIAL",         # Number of social workers
    "IN_PROF_SAUDE",                 # Health professional available
    "QT_PROF_SAUDE",                 # Number of health professionals
    "IN_PROF_COORDENADOR",           # Pedagogical coordinator available
    "QT_PROF_COORDENADOR",           # Number of pedagogical coordinators

    # Enrollment by education level
    "QT_MAT_INF",                    # Number of students in early childhood education
    "QT_MAT_FUND",                   # Number of students in elementary education
    "QT_MAT_MED"                     # Number of students in high school
]

num_cols = ["QT_MAT_INF", "QT_MAT_FUND", "QT_MAT_MED", "QT_PROF_COORDENADOR", "QT_PROF_SAUDE", "QT_PROF_ASSIST_SOCIAL", "QT_PROF_PSICOLOGO", "QT_COMP_PORTATIL_ALUNO", "QT_DESKTOP_ALUNO"]

years = [2020, 2021, 2022, 2023, 2024]

col_dim_esc = [
    "CO_ENTIDADE",
    "NU_ANO_CENSO", 
    "NO_ENTIDADE", 
    "SG_UF", 
    "CO_UF", 
    "CO_MUNICIPIO", 
    "NO_MUNICIPIO", 
    "TP_DEPENDENCIA", 
    "TP_LOCALIZACAO"]

col_fato = [
    "CO_ENTIDADE",
    "NU_ANO_CENSO",
    "QT_MAT_INF",
    "QT_MAT_FUND",
    "QT_MAT_MED",
    "QT_PROF_COORDENADOR",
    "QT_PROF_SAUDE",
    "QT_PROF_ASSIST_SOCIAL",
    "QT_PROF_PSICOLOGO",
    "QT_COMP_PORTATIL_ALUNO",
    "QT_DESKTOP_ALUNO"]

col_dim_infra = [
    "CO_ENTIDADE",
    "NU_ANO_CENSO",
    "IN_AGUA_POTAVEL",
    "IN_ENERGIA_REDE_PUBLICA",
    "IN_ESGOTO_REDE_PUBLICA",
    "IN_LIXO_SERVICO_COLETA",
    "IN_TRATAMENTO_LIXO_SEPARACAO",
    "IN_TRATAMENTO_LIXO_REUTILIZA",
    "IN_TRATAMENTO_LIXO_RECICLAGEM",
    "IN_BANHEIRO",
    "IN_BANHEIRO_PNE",
    "IN_REFEITORIO",
    "IN_ALIMENTACAO",
    "IN_QUADRA_ESPORTES",
    "IN_QUADRA_ESPORTES_COBERTA",
    "IN_BIBLIOTECA",
    "IN_LABORATORIO_CIENCIAS",
    "IN_LABORATORIO_INFORMATICA",
    "IN_MEDIACAO_EAD",
    "IN_MEDIACAO_SEMIPRESENCIAL",
    "IN_MEDIACAO_PRESENCIAL",
    "IN_INTERNET",
    "IN_COMPUTADOR",
    "IN_EQUIP_MULTIMIDIA",
    "IN_EQUIP_TV",
    "IN_PROF_PSICOLOGO",
    "IN_PROF_ASSIST_SOCIAL",
    "IN_PROF_SAUDE",
    "IN_PROF_COORDENADOR"]
