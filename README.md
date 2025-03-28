graph TD
    subgraph Cloud VM (Entorno de Ejecución Remoto)
        A(Inicio: Tarea Programada 'cron') -- Dispara a la hora definida (ej: 3:00 AM) --> B{Ejecutar Script ETL Python};

        B -- 1. Inicia ejecución --> C[Leer Configuración / Credenciales];
        C -- Desde archivo '.env' --> D(Script ETL: tu_script.py);

        D -- 2. Extracción --> E{Conectar a Fuentes de Datos};
        E -- ¿Conexión Exitosa? --> F[Extraer Datos Crudos];
        E -- No --> LogError1[Registrar Error de Conexión Fuentes en Log];
        LogError1 --> Z(Fin del Proceso Diario con Error);

        F -- Datos Crudos --> G[Transformar Datos];
        G -- Datos Transformados --> H{Conectar a Supabase DB};

        H -- Usando Credenciales de '.env' --> I(Base de Datos Supabase - PostgreSQL);
        H -- ¿Conexión Exitosa? --> J[Cargar Datos Transformados];
        H -- No --> LogError2[Registrar Error de Conexión Supabase en Log];
        LogError2 --> Z;

        J -- Escribir en Tablas --> I;
        J -- ¿Carga Exitosa? --> K[Registrar Éxito en Log];
        J -- No --> LogError3[Registrar Error de Carga en Log];
        LogError3 --> Z;

        K -- Proceso completado --> Y(Fin del Proceso Diario Exitoso);

        %% Referencias a archivos y entorno
        D --> EnvFile([.env File]);
        K --> LogFile([etl_log.log]);
        LogError1 --> LogFile;
        LogError2 --> LogFile;
        LogError3 --> LogFile;

        B -- Corre dentro de --> Venv(Entorno Virtual Python 'venv');
    end

    %% Fuentes de Datos Externas (Fuera de la VM)
    subgraph Fuentes de Datos
        SourceDB[(Bases de Datos / APIs / Archivos)];
    end

    %% Conexiones Externas
    E --> SourceDB;
    F --> SourceDB;
    H --> SupabaseCloud[(Supabase Cloud Platform)];
    I --> SupabaseCloud;

    style LogError1 fill:#f9f,stroke:#333,stroke-width:2px
    style LogError2 fill:#f9f,stroke:#333,stroke-width:2px
    style LogError3 fill:#f9f,stroke:#333,stroke-width:2px
    style Z fill:#f99,stroke:#333,stroke-width:2px
    style Y fill:#9cf,stroke:#333,stroke-width:2px
