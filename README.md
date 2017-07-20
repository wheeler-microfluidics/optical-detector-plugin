# optical-detector-plugin

This is a MicroDrop plugin for collecting optical density (OD) and fluorescence readings using an Arduino-based [pulse counter](https://github.com/wheeler-microfluidics/pulse-counter-rpc) and a light-intensity-to-frequency IC (e.g., TSL230R).

Optional support for running one of two different sub-protocols on any given step, conditional on the OD reading. To choose which sub-protocol to run for under/over threshold events on the currently selected step, choose the menu item **`Tools/OD threshold events`**.
