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
import logging
from datetime import datetime

import gobject
import gtk
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

logger = logging.getLogger(__name__)


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
        # Timeout
        Integer.named('dmf_control_timeout_ms').using(optional=True,
                                                      default=5000),
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
                                                         default=9),
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
        .using(default=0, optional=True, validators=[ValueAtLeast(minimum=0)],
               properties={'title': 'Abs samples'}),
        Integer.named('absorbance_sample_duration_ms')
        .using(default=1000, optional=True,
               validators=[ValueAtLeast(minimum=0)],
               properties={'title': 'Abs ms'}),
        Float.named('absorbance_excitation_intensity')
        .using(default=23, optional=True,
               validators=[ValueAtLeast(minimum=0), ValueAtMost(maximum=100)],
               properties={'title': 'Abs %'}),
        # Fluorescence detector 1 settings
        Integer.named('fluorescence_1_sample_count')
        .using(default=0, optional=True, validators=[ValueAtLeast(minimum=0)],
               properties={'title': 'Fl1 samples'}),
        Integer.named('fluorescence_1_sample_duration_ms')
        .using(default=1000, optional=True,
               validators=[ValueAtLeast(minimum=0)],
               properties={'title': 'Fl1 ms'}),
        Float.named('fluorescence_1_excitation_intensity')
        .using(default=100, optional=True,
               validators=[ValueAtLeast(minimum=0), ValueAtMost(maximum=100)],
               properties={'title': 'Fl1 %'}),
        # Fluorescence detector 2 settings
        Integer.named('fluorescence_2_sample_count')
        .using(default=0, optional=True, validators=[ValueAtLeast(minimum=0)],
               properties={'title': 'Fl2 samples'}),
        Integer.named('fluorescence_2_sample_duration_ms')
        .using(default=1000, optional=True,
               validators=[ValueAtLeast(minimum=0)],
               properties={'title': 'Fl2 ms'}),
        Float.named('fluorescence_2_excitation_intensity')
        .using(default=100, optional=True,
               validators=[ValueAtLeast(minimum=0), ValueAtMost(maximum=100)],
               properties={'title': 'Fl2 %'}),
    )

    def __init__(self):
        self.name = self.plugin_name
        self.proxy = None
        self.control_board_timeout_id = None

    def verify_connected(self):
        if self.proxy is None:
            try:
                self.proxy = SerialProxy()
                logger.info('[OpticalDetectorPlugin] proxy connected')
            except (Exception, ), exception:
                warnings.warn(str(exception))
                logger.warning('[OpticalDetectorPlugin] could not connect to '
                               'pulse counter')
                return False
        return True

    def get_schedule_requests(self, function_name):
        """
        Returns a list of scheduling requests (i.e., ScheduleRequest
        instances) for the function specified by function_name.
        """
        if function_name in ['on_step_run']:
            # Execute `on_step_run` before control board.
            return [ScheduleRequest(self.name, 'wheelerlab.dmf_control_board')]
        return []

    def on_plugin_disable(self):
        """
        Handler called once the plugin instance is disabled.
        """
        if self.proxy is not None:
            del self.proxy
            self.proxy = None
            logger.info('[OpticalDetectorPlugin] disconnected from proxy')

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
        if not self.verify_connected():
            logger.warning("Warning: No pulse counter device connection.")

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
        self._kill_running_step()

        # At start of step, set flag to indicate that we are waiting for the
        # control board to complete the current step before acquiring
        # measurements.
        self.control_board_step_complete = False

        # Record current time to enable timeout if the control board takes too
        # long.
        self.start_time = datetime.now()

        # Schedule check to see if control board has completed current step.
        gtk.idle_add(self._wait_for_control_board)
        self.control_board_timeout_id = \
            gobject.timeout_add(100, self._wait_for_control_board, True)

    def _kill_running_step(self):
        if self.control_board_timeout_id is not None:
            gobject.source_remove(self.control_board_timeout_id)
            self.control_board_timeout_id = None

    def _wait_for_control_board(self, continue_=False):
        '''
        After control board has completed current step, measure pulse counts
        and save to experiment log.

        Args:

            continue_ (bool) : Value to return if control board has not
                completed current step.  This can be used, for example, to
                repeat as a scheduled timeout callback.

        Returns:

            (bool) : `False` if step is complete; value of `continue_`
                otherwise.
        '''
        if not self.control_board_step_complete:
            # Control board has not completed current step.
            app_values = self.get_app_values()
            timeout = app_values['dmf_control_timeout_ms']
            if timeout > 0 and timeout < (datetime.now() -
                                          self.start_time).total_seconds():
                # Timed out waiting for control board.
                emit_signal('on_step_complete', [self.name, 'Fail'])
                return False
            return continue_
        else:
            self._kill_running_step()

            if self.verify_connected():
                self.count_pulses_and_log()

            # Signal step completion.
            emit_signal('on_step_complete', [self.name, None])
            return False

    def count_pulses_and_log(self):
        # Connected to pulse counter, so measure
        app = get_app()
        app_values = self.get_app_values()
        options = self.get_step_options()

        step_results = []

        for k in ['absorbance', 'fluorescence_1', 'fluorescence_2']:
            if options[k + '_sample_count']:
                results = self.measure_pulses(k, app_values, options)
                step_results.extend(results)

        if step_results:
            # Create data frame containing all sample results from current step
            df_results = pd.DataFrame(step_results, columns=['detector',
                                                             'sample_i',
                                                             'intensity',
                                                             'duration_ms',
                                                             'pulse_count'])
            app.experiment_log.add_data({'pulse_counts': df_results}, self.name)

    def on_step_complete(self, plugin_name, return_value=None):
        if return_value is None and (plugin_name ==
                                     'wheelerlab.dmf_control_board'):
            logger.info('Control board has completed step.')
            # Set flag to indicate that control board has completed current
            # step.
            self.control_board_step_complete = True

PluginGlobals.pop_env()
