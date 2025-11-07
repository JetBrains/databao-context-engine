# How do we create and query embeddings?

The following diagrams document the way we build context, create and query embeddings in our plugin architecture.

```mermaid
---
title: Build Context
---
sequenceDiagram
    participant NemoryBuildCommand
    participant BuildPlugin
    participant FileSystem
    
    NemoryBuildCommand->>NemoryBuildCommand: Discover installed plugins
    NemoryBuildCommand->>FileSystem: Iterate on config files in the src directory
    activate FileSystem
    deactivate FileSystem
    FileSystem-->NemoryBuildCommand: 
    loop for each config file
        NemoryBuildCommand->>FileSystem: Parse the yaml file and read the source type
        activate FileSystem
        deactivate FileSystem
        FileSystem-->NemoryBuildCommand: 
        NemoryBuildCommand->>NemoryBuildCommand: Find the plugin<br/>for that source type
        NemoryBuildCommand->>BuildPlugin: Execute the plugin for that config
        activate BuildPlugin
        BuildPlugin-->NemoryBuildCommand: Returns the build result for that source
        deactivate BuildPlugin
        NemoryBuildCommand->>FileSystem: Write the output in a YAML file
        Note over NemoryBuildCommand,FileSystem: Next step: Build Embeddings<br/>for this data source
    end
```


```mermaid
---
title: Build Embeddings for a data source
---
sequenceDiagram
    participant NemoryBuildCommand
    participant BuildPlugin
    participant OllamaService
    participant DuckDB
    
    Note over NemoryBuildCommand,DuckDB: Input: Build result for a data source
    NemoryBuildCommand->>BuildPlugin: Get the list of chunks for the build result
    activate NemoryBuildCommand
    activate BuildPlugin
    deactivate BuildPlugin
    BuildPlugin-->NemoryBuildCommand: 
    NemoryBuildCommand->>OllamaService: Embed each chunk (create the vector representation)
    activate OllamaService
    deactivate OllamaService
    OllamaService-->NemoryBuildCommand:  
    NemoryBuildCommand->>DuckDB: Insert the embedded chunks, with their content
    activate DuckDB
    deactivate DuckDB
    DuckDB-->NemoryBuildCommand: 
    deactivate NemoryBuildCommand
```

```mermaid
---
title: Query Embeddings for a prompt
---
sequenceDiagram
    participant NemoryBuildCommand
    participant OllamaService
    participant DuckDB
    participant FileSystem
    
    opt If no run-id provided
        NemoryBuildCommand->>FileSystem: Find latest run directory
        activate FileSystem
        deactivate FileSystem
        FileSystem-->NemoryBuildCommand: 
    end
    NemoryBuildCommand->>DuckDB: Connect to the DB of the run
    activate DuckDB
    NemoryBuildCommand->>OllamaService: Embed the query prompt (create the vector representation)
    activate OllamaService
    deactivate OllamaService
    OllamaService-->NemoryBuildCommand: 
    NemoryBuildCommand->>DuckDB: Find nearest chunks to the prompt, with their content
    DuckDB-->NemoryBuildCommand: 
    deactivate DuckDB
    NemoryBuildCommand->>NemoryBuildCommand: Concatenate<br/>all chunk content
```

```mermaid
---
title: Embeddings DB
---
classDiagram
class embeddings { 
    varchar(0) chunk_id
    float[768](0) vec
    varchar(0) embedded_text
    varchar(0) chunk_content
}
```