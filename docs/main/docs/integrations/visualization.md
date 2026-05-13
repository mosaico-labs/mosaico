---
title: Visualization
sidebar_position: 1
description: "Mosaico's approach to data visualization. Covers the first-class integrations and the reasoning behind Mosaico's focus on the data stack rather than building its own visualization tooling."
---

Mosaico will not ship a visualization tool. The open-source robotics ecosystem already has excellent options for this, and building a competing tool would be a distraction from what Mosaico is designed to do: reliably store, index, and retrieve high-frequency sensor data at scale. 

Our focus is the data stack.

## PlotJuggler

[PlotJuggler](https://github.com/PlotJuggler/PlotJuggler) ships with first-class integration support for Mosaico. You can connect it directly to a running `mosaicod` instance, browse the sequence catalog, and stream any topic into the timeline for inspection and plotting, without writing any code or exporting files.

PlotJuggler is free, open-source, and widely used across the ROS community. If you are working with time-series sensor data, it is the recommended starting point.
