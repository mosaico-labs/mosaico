# Questions

Answer each one as you would to someone who has just started with Mosaico and its Python SDK. Cite the
documentation page (path from the repository root) for every claim.

1. What is the difference between a topic and a sequence? Can one topic hold IMU and GPS data together?
2. Why does Mosaico have sessions instead of versions, and how do I add new results to a sequence that
   is already stored?
3. What is `mosaicod`, and where does it keep my data? If its database is destroyed, is the data gone?
4. Why does every piece of data need a declared type in Mosaico? And what if my data has no Python class?
5. I want to look at what a sequence contains before downloading gigabytes. What do handlers do, and what
   do streamers do?
6. What can I filter on when I query Mosaico, and how are several filters combined?
7. Can I give a colleague an API key that can only read one sequence? Does a `write` key also read?
8. My IMU runs at 100 Hz and my GPS at 5 Hz. How does Mosaico turn them into a table a model can train
   on?
9. I have ROS 2 bags saved as `.mcap` and other MCAP files encoded with protobuf. Which bridge ingests
   which?
10. Our robot drops a recording into a folder after every run. How do I get them into Mosaico without
    launching an injector by hand each time?
11. Can I browse the catalog without writing Python? How does the command decide which server to talk to?
12. Does Mosaico come with a tool to plot my data?
13. I want to find every recording where the IMU acceleration on z went above 20 m/s². Which page or
    skill should I use?
14. I have not installed anything yet. Where do I start, and which skill helps me get connected?
15. I need PyTorch training batches out of my sequences. Which page or skill should I use?
