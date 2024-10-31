<h1>Simple SQL DB</h1><p>&nbsp;</p><p>An extremely simple and naive SQL database, written in Go with a CLI and REST API to interface with it.<br><br>I wrote the entire project from scratch, designing and building my own custom <code>SQL Engine</code>, <code>Database File Manager</code>, and <code>Frontend Interface Layers</code>. I was inspired to create this project after reading <a target="_blank" rel="noopener noreferrer" href="https://cstack.github.io/db_tutorial/">this tutorial</a>.</p><p>&nbsp;</p><h2>Features</h2><p>&nbsp;</p><p><strong>Integrated Engine and File Manager</strong></p><p>&nbsp;</p><p>Database tables are stored using a custom file format and binary encodings, the engine utilizes this structure for increased table scanning speeds and instantaneous index-based row access.</p><p>&nbsp;</p><p><strong>Custom SQL Parser</strong></p><p>&nbsp;</p><p>Implemented mostly using a push-down automata, producing an Abstract Syntax Tree that is directly executable by the engine and also capable of basic data-structure-aware optimizations.</p><p>&nbsp;</p><p>&nbsp;</p><p>&nbsp;</p>