"""
Copyright 2015 Christian Fobel

This file is part of optical_detector_plugin.

optical_detector_plugin is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

dmf_control_board is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with optical_detector_plugin.  If not, see <http://www.gnu.org/licenses/>.
"""
import warnings

import pandas as pd
from path_helpers import path
from pulse_counter_rpc import SerialProxy
from flatland import Form, Integer, Float
from flatland.validation import ValueAtLeast, ValueAtMost
from microdrop.plugin_helpers import (AppDataController, StepOptionsController,
                                      get_plugin_info)
from microdrop.plugin_manager import (PluginGlobals, Plugin, IPlugin,
                                      ScheduleRequest, implements, emit_signal)
from microdrop.app_context import get_app
from microdrop.logger import logger


PluginGlobals.push_env('microdrop.managed')


class OpticalDetectorPlugin(Plugin, AppDataController, StepOptionsController):
    """
    This class is automatically registered with the PluginManager.
    """
    implements(IPlugin)
    version = get_plugin_info(path(__file__).parent).version
    plugin_name = get_plugin_info(path(__file__).parent).plugin_name

    '''
    AppFields
    ---------

    A flatland Form specifying application options for the current plugin.
    Note that nested Form objects are not supported.

    Since we subclassed AppDataController, an API is available to access and
    modify these attributes.  This API also provides some nice features
    automatically:
        -all fields listed here will be included in the app options dialog
            (unless properties=dict(show_in_gui=False) is used)
        -the values of these fields will be stored persistently in the microdrop
            config file, in a section named after this plugin's name attribute
    '''
    AppFields = Form.of(
        # Pulse counting pin
        Integer.named('pulse_count_pin').using(optional=True, default=2),
        # Multiplexer channels
        Integer.named('absorbance_channel').using(optional=True, default=0),
        Integer.named('fluorescence_1_channel').using(optional=True,
                                                      default=1),
        Integer.named('fluorescence_2_channel').using(optional=True,
                                                      default=2),
        # Excitation pins (e.g., LED control)
        Integer.named('absorbance_excite_pin').using(optional=True, default=5),
        Integer.named('fluorescence_1_excite_pin').using(optional=True,
                                                         default=6),
        Integer.named('fluorescence_2_excite_pin').using(optional=True,
                                                         default=7),
    )

    '''
    StepFields
    ---------

    A flatland Form specifying the per step options for the current plugin.
    Note that nested Form objects are not supported.

    Since we subclassed StepOptionsController, an API is available to access and
    modify these attributes.  This API also provides some nice features
    automatically:
        -all fields listed here will be included in the protocol grid view
            (unless properties=dict(show_in_gui=False) is used)
        -the values of these fields will be stored persistently for each step
    '''
    StepFields = Form.of(
        # Absorbance detector settings
        Integer.named('absorbance_sample_count')
        .using(default=0, optional=True, validators=[ValueAtLeast(minimum=0)]),
        Integer.named('absorbance_sample_duration_ms')
        .using(default=1000, optional=True,
               validators=[ValueAtLeast(minimum=0)]),
        Float.named('absorbance_excitation_intensity')
        .using(default=23, optional=True,
               validators=[ValueAtLeast(minimum=0), ValueAtMost(maximum=100)]),
        # Fluorescence detector 1 settings
        Integer.named('fluorescence_1_sample_count')
        .using(default=0, optional=True, validators=[ValueAtLeast(minimum=0)]),
        Integer.named('fluorescence_1_sample_duration_ms')
        .using(default=1000, optional=True,
               validators=[ValueAtLeast(minimum=0)]),
        Float.named('fluorescence_1_excitation_intensity')
        .using(default=100, optional=True,
               validators=[ValueAtLeast(minimum=0), ValueAtMost(maximum=100)]),
        # Fluorescence detector 2 settings
        Integer.named('fluorescence_2_sample_count')
        .using(default=0, optional=True, validators=[ValueAtLeast(minimum=0)]),
        Integer.named('fluorescence_2_sample_duration_ms')
        .using(default=1000, optional=True,
               validators=[ValueAtLeast(minimum=0)]),
        Float.named('fluorescence_2_excitation_intensity')
        .using(default=100, optional=True,
               validators=[ValueAtLeast(minimum=0), ValueAtMost(maximum=100)]),
    )

    def __init__(self):
        self.name = self.plugin_name
        self.proxy = None

    def verify_connected(self):
        if self.proxy is None:
            try:
                self.proxy = SerialProxy()
                logger.info('[OpticalDetectorPlugin] proxy connected')
            except (Exception, ), exception:
                warnings.warn(str(exception))
                return False
        return True

    def get_schedule_requests(self, function_name):
        """
        Returns a list of scheduling requests (i.e., ScheduleRequest
        instances) for the function specified by function_name.
        """
        if function_name in ['on_step_options_swapped']:
            return [ScheduleRequest('wheelerlab.dmf_control_board', self.name)]
        return []

    def on_plugin_disable(self):
        """
        Handler called once the plugin instance is disabled.
        """
        if self.proxy is not None:
            del self.proxy
            self.proxy = None

    def on_plugin_enable(self):
        """
        Handler called once the plugin instance is enabled.

        Note: if you inherit your plugin from AppDataController and don't
        implement this handler, by default, it will automatically load all
        app options from the config file. If you decide to overide the
        default handler, you should call:

            AppDataController.on_plugin_enable(self)

        to retain this functionality.
        """
        self.verify_connected()
        super(OpticalDetectorPlugin, self).on_plugin_enable()

    def on_protocol_run(self):
        """
        Handler called when a protocol starts running.
        """
        pass

    def on_step_options_swapped(self, plugin, old_step_number, step_number):
        """
        Handler called when the step options are changed for a particular
        plugin.  This will, for example, allow for GUI elements to be
        updated based on step specified.

        Parameters:
            plugin : plugin instance for which the step options changed
            step_number : step number that the options changed for
        """
        if self.verify_connected():
            app = get_app()
            app_values = self.get_app_values()
            options = self.get_step_options(step_number=step_number)

            step_results = {}

            for k in ['absorbance', 'fluorescence_1', 'fluorescence_2']:
                step_results[k] = self.measure_pulses(k, app_values, options)

            app.experiment_log.add_data(step_results, self.name)

    def measure_pulses(self, detector_name, app_values, step_options):
        '''
        Measure samples from detector
        '''
        # Set excitation intensity
        intensity = step_options[detector_name + '_excitation_intensity']
        intensity_duty_cycle = int(intensity / 100. * 255)

        self.proxy.analog_write(app_values[detector_name + '_excite_pin'],
                                intensity_duty_cycle)

        # Measure selected number pulse count samples.
        results = []
        duration_ms = step_options[detector_name + '_sample_duration_ms']

        for i in xrange(step_options[detector_name + '_sample_count']):
            # Take measurement
            result = self.proxy.count_pulses(
                app_values['pulse_count_pin'],
                app_values[detector_name + '_channel'],
                duration_ms,
                timeout_s=3 * duration_ms)
            results.append([detector_name, i, intensity, duration_ms, result])

        # Turn off excitation
        self.proxy.analog_write(app_values[detector_name + '_excite_pin'], 0)
        return results

    def on_step_run(self):
        """
        Handler called whenever a step is executed. Note that this signal
        is only emitted in realtime mode or if a protocol is running.

        Plugins that handle this signal must emit the on_step_complete
        signal once they have completed the step. The protocol controller
        will wait until all plugins have completed the current step before
        proceeding.

        return_value can be one of:
            None
            'Repeat' - repeat the step
            or 'Fail' - unrecoverable error (stop the protocol)
        """
        if self.verify_connected():
            # Connected to pulse counter, so measure
            app = get_app()
            app_values = self.get_app_values()
            options = self.get_step_options()

            step_results = []

            for k in ['absorbance', 'fluorescence_1', 'fluorescence_2']:
                if options[k + '_sample_count']:
                    results = self.measure_pulses(k, app_values, options)
                    step_results.extend(results)

            # Create data frame containing all sample results from current step
            df_results = pd.DataFrame(step_results, columns=['detector',
                                                             'sample_i',
                                                             'intensity',
                                                             'duration_ms',
                                                             'pulse_count'])
            app.experiment_log.add_data(df_results, self.name)

        # Signal step completion.
        emit_signal('on_step_complete', [self.name, None])


PluginGlobals.pop_env()
