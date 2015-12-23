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
import time

from flatland import Form, Integer, Float
from flatland.validation import ValueAtLeast, ValueAtMost
from path_helpers import path
from pulse_counter_rpc import SerialProxy
from pygtkhelpers.ui.extra_widgets import Filepath
from pygtkhelpers.ui.form_view_dialog import FormViewDialog
from pygtkhelpers.ui.objectlist import PropertyMapper
import gobject
import gtk
import pandas as pd
import numpy as np
from microdrop.plugin_helpers import (AppDataController, StepOptionsController,
                                      get_plugin_info)
from microdrop.plugin_manager import (PluginGlobals, Plugin, IPlugin,
                                      IWaveformGenerator, ScheduleRequest,
                                      implements, emit_signal,
                                      get_service_instance_by_name)
from microdrop.app_context import get_app
from microdrop.protocol import Protocol

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
        Integer.named('absorbance_count_pin').using(optional=True, default=2),
        Integer.named('fluorescence_1_count_pin').using(optional=True,
                                                        default=3),
        # Multiplexer channels
        Integer.named('absorbance_channel').using(optional=True, default=1),
        Integer.named('fluorescence_1_channel').using(optional=True,
                                                      default=2),
        # Excitation pins (e.g., LED control)
        Integer.named('absorbance_excite_pin').using(optional=True,
                                                     default=10),
        Integer.named('fluorescence_1_excite_pin').using(optional=True,
                                                         default=5),
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
    # Create `PropertyMapper` instances to only make step option columns
    # editable if the number of sample counts is greater than 0 for the
    # corresponding detector.
    active_mappers = dict([(k, [PropertyMapper(a, attr=k + '_sample_count',
                                               format_func=lambda v: v > 0)
                                for a in ['sensitive', 'editable']])
                           for k in ['absorbance', 'fluorescence_1']])

    StepFields = Form.of(
        # Absorbance detector settings
        Integer.named('absorbance_sample_count')
        .using(default=0, optional=True, validators=[ValueAtLeast(minimum=0)],
               properties={'title': 'Abs samples'}),
        Integer.named('absorbance_sample_duration_ms')
        .using(default=1000, optional=True,
               validators=[ValueAtLeast(minimum=0)],
               properties={'title': 'Abs ms',
                           'mappers': active_mappers['absorbance']}),
        Float.named('absorbance_excitation_intensity')
        .using(default=23, optional=True,
               validators=[ValueAtLeast(minimum=0), ValueAtMost(maximum=100)],
               properties={'title': 'Abs %',
                           'mappers': active_mappers['absorbance']}),
        Float.named('absorbance_threshold')
        .using(default=0, optional=True, validators=[ValueAtLeast(minimum=0)],
               properties={'title': 'Abs threshold',
                           'mappers': active_mappers['absorbance']}),
        # Fluorescence detector 1 settings
        Integer.named('fluorescence_1_sample_count')
        .using(default=0, optional=True, validators=[ValueAtLeast(minimum=0)],
               properties={'title': 'Fl1 samples'}),
        Integer.named('fluorescence_1_sample_duration_ms')
        .using(default=1000, optional=True,
               validators=[ValueAtLeast(minimum=0)],
               properties={'title': 'Fl1 ms',
                           'mappers': active_mappers['fluorescence_1']}),
        Float.named('fluorescence_1_excitation_intensity')
        .using(default=100, optional=True,
               validators=[ValueAtLeast(minimum=0), ValueAtMost(maximum=100)],
               properties={'title': 'Fl1 %',
                           'mappers': active_mappers['fluorescence_1']}),
    )

    def __init__(self):
        self.name = self.plugin_name
        self.control_board = None
        self.proxy = None
        self.control_board_timeout_id = None
        self.initialized = False

    def verify_connected(self):
        if self.proxy is None:
            try:
                self.proxy = SerialProxy()
                logger.info('[OpticalDetectorPlugin] proxy connected')
            except (Exception, ), exception:
                warnings.warn(str(exception))
                logger.info('[OpticalDetectorPlugin] proxy connected',
                            exc_info=True)
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

    def _create_menu(self):
        app = get_app()
        self.od_sensor_menu_item = gtk.MenuItem('OD threshold events')
        app.main_window_controller.menu_tools.append(self.od_sensor_menu_item)
        self.od_sensor_menu_item.connect('activate',
                                         self.on_edit_od_threshold_events)
        self.od_sensor_menu_item.show()

    def on_edit_od_threshold_events(self, widget, data=None):
        """
        Handler called when the user clicks on "Edit OD threshold events" in
        the "Tools" menu.
        """
        app = get_app()
        options = self.get_step_options()
        form = Form.of(*[Filepath.named(k).using(default=options.get(k, None),
                                                 optional=True)
                         for k in ('under_threshold_subprotocol',
                                   'over_threshold_subprotocol')])
        dialog = FormViewDialog()
        valid, response =  dialog.run(form)

        step_options_changed = False
        if valid:
            for k in ('under_threshold_subprotocol',
                      'over_threshold_subprotocol'):
                if response[k] and response[k] != options.get(k, None):
                    options[k] = response[k]
                    step_options_changed = True
        if step_options_changed:
            emit_signal('on_step_options_changed',
                        [self.name, app.protocol.current_step_number],
                        interface=IPlugin)

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
        # get a reference to the control board
        if self.control_board is None:
            try:
                plugin = get_service_instance_by_name('wheelerlab'
                                                      '.dmf_control_board')
            except:
                logging.warning('Could not get connection to control board.')
            else:
                self.control_board = plugin.control_board
        if not self.verify_connected():
            logger.warning('[OpticalDetectorPlugin] could not connect to '
                           'pulse counter')
        self._create_menu()
        self.initialized = True
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
                app_values[detector_name + '_count_pin'],
                app_values[detector_name + '_channel'],
                duration_ms)
            results.append([datetime.now(), detector_name, i, intensity,
                            duration_ms, result])

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
                df_results = self.count_pulses_and_log()
                print df_results
                if df_results is not None:
                    df_absorbance = df_results.loc[df_results.detector ==
                                                   'absorbance']
                    if df_absorbance.shape[0] > 0:
                        # TODO For now, we're actually setting threshold based
                        # on *intensity*.
                        absorbance = (df_absorbance.pulse_count /
                                      df_absorbance.duration_ms *
                                      1e-3).median()
                        try:
                            self.process_absorbance(absorbance)
                        except IOError:
                            logging.error('Cannot process absorbance '
                                          'measurement.', exc_info=True)

            # Signal step completion.
            emit_signal('on_step_complete', [self.name, None])
            return False

    def process_absorbance(self, absorbance):
        if self.control_board is None or not self.control_board.connected():
            #raise IOError('No control board connection.')
            warnings.warn('No control board connection.')

        options = self.get_step_options()
        sub_protocol = []
        if absorbance >= options['absorbance_threshold']:
            sub_protocol_path = options.get('over_threshold_subprotocol', None)
            if sub_protocol_path:
                logger.info('[ODSensorPlugin] absorbance >= threshold, run '
                            'subprotocol %s' % sub_protocol_path)
                sub_protocol = Protocol.load(sub_protocol_path)
        else:
            sub_protocol_path = options.get('under_threshold_subprotocol',
                                            None)
            if sub_protocol_path:
                logger.info('[ODSensorPlugin] absorbance < threshold, run '
                            'subprotocol %s' % sub_protocol_path)
                sub_protocol = Protocol.load(sub_protocol_path)

        # Execute all steps in sub protocol
        for i, step in enumerate(sub_protocol):
            logger.info('[ODSensorPlugin] subprotocol step %d' % i)
            # TODO No true sub protocol support.  For now, just hijack control
            # board and set voltage, frequency and channel states directly.
            dmf_options = step.get_data('microdrop.gui.dmf_device_controller')
            options = step.get_data('wheelerlab.dmf_control_board')

            state = dmf_options.state_of_channels
            max_channels = self.control_board.number_of_channels()

            if len(state) >  max_channels:
                state = state[0:max_channels]
            elif len(state) < max_channels:
                state = np.concatenate([state,
                        np.zeros(max_channels - len(state), int)])
            else:
                assert(len(state) == max_channels)

            emit_signal("set_voltage", options.voltage,
                        interface=IWaveformGenerator)
            emit_signal("set_frequency", options.frequency,
                        interface=IWaveformGenerator)
            self.control_board.set_state_of_all_channels(state)
            time.sleep(options.duration * 1e-3)

    def count_pulses_and_log(self):
        # Connected to pulse counter, so measure
        app = get_app()
        app_values = self.get_app_values()
        options = self.get_step_options()

        step_results = []

        for k in ['absorbance', 'fluorescence_1']:
            if options[k + '_sample_count']:
                results = self.measure_pulses(k, app_values, options)
                step_results.extend(results)

        if step_results:
            # Create data frame containing all sample results from current step
            df_results = pd.DataFrame(step_results, columns=['timestamp',
                                                             'detector',
                                                             'sample_i',
                                                             'intensity',
                                                             'duration_ms',
                                                             'pulse_count'])
            app.experiment_log.add_data({'pulse_counts': df_results}, self.name)
            return df_results

    def on_step_complete(self, plugin_name, return_value=None):
        if return_value is None and (plugin_name ==
                                     'wheelerlab.dmf_control_board'):
            logger.info('Control board has completed step.')
            # Set flag to indicate that control board has completed current
            # step.
            self.control_board_step_complete = True

PluginGlobals.pop_env()
