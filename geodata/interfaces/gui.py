try:
    from geocompare.engine import Engine
except ImportError:  # pragma: no cover - script execution fallback
    try:
        from geodata.engine import Engine
    except ImportError:  # pragma: no cover - script execution fallback
        from engine import Engine


import tkinter as tk
import tkinter.messagebox as tk_messagebox

from tkinter import ttk

import time

from functools import partial

class GeodataGUI:
    def __init__(self):
        self.dp_offset = 0

        self.root = tk.Tk()
        self.root.title("GeoCompare")
        self.root.minsize(500, 20)

        self.engine = Engine()

        self.dp_fetch_one = self.engine.d['demographicprofiles'][0]

        # These attributes may or may not have not been created, and they
        # may or may not have been destroyed.
        self.evs_within_geo_label = None
        self.evs_within_state_combobox = None
        self.evs_within_county_combobox = None
        self.evs_zcta_entry = None

        # Main window #########################################################

        ### Upper area
        self.upper_frame = tk.Frame(master=self.root)

        ## Search area

        # Search Entry
        self.search_entry_placeholder = False

        self.search_entry = tk.Entry(master=self.upper_frame, font='Arial 9 italic')
        self.search_entry.insert(0, 'Search for a geography')
        self.search_entry_placeholder = True
        self.search_entry.pack(side=tk.LEFT, fill=tk.BOTH, expand=tk.YES)
        self.search_entry.bind('<Key>', self.search_key)
        self.search_entry.bind('<KeyRelease>', self.search_keyrelease)
        self.search_entry.bind('<FocusIn>', self.search_focusin)
        self.search_entry.icursor(0)

        self.search_button = tk.Button(master=self.upper_frame, text='Search', padx=10, command=self.search)
        self.search_button.pack(side=tk.LEFT)

        self.go_button = tk.Button(master=self.upper_frame, text='Go', bg='green', fg='white', padx=10, command=self.dp_go)
        self.go_button.pack(side=tk.LEFT)
        self.root.bind('<Return>', self.dp_go)

        # Pack upper_frame
        self.upper_frame.pack(fill=tk.BOTH, expand=tk.YES, pady=10)
        self.search_entry.focus_set()

        ### Middle area
        self.middle_frame = tk.Frame(master=self.root)

        self.middle_frame_separator = ttk.Separator(master=self.middle_frame, orient=tk.HORIZONTAL)
        self.middle_frame_separator.pack(fill=tk.X, expand=tk.YES)

        ## Primary extreme values selection area
        self.evs_frame = tk.Frame(master=self.middle_frame)

        self.evs_display_label = tk.Label(master=self.evs_frame, text='Display', anchor='w')
        self.evs_display_label.pack(side=tk.LEFT, padx=(0, 5), pady=5)

        self.evs_combobox = ttk.Combobox(master=self.evs_frame, state='readonly')
        self.evs_combobox['values'] = ['highest values',
                                       'lowest values']
        self.evs_combobox.set('highest values')
        self.evs_combobox.pack(side=tk.LEFT)

        self.evs_for_label = tk.Label(master=self.evs_frame, text='for', padx=5)
        self.evs_for_label.pack(side=tk.LEFT)

        self.evs_comp_combobox = ttk.Combobox(master=self.evs_frame, state='readonly')
        self.evs_comp_combobox['values'] = list(self.dp_fetch_one.rl.values())
        self.evs_comp_combobox.set('Total population')
        self.evs_comp_combobox.pack(side=tk.LEFT)

        self.evs_go_button = tk.Button(master=self.evs_frame, text='Go', bg='green', fg='white', padx=10, command=self.evs_go)
        self.evs_go_button.pack(side=tk.RIGHT)

        self.evs_frame.pack(fill=tk.X, expand=tk.YES, pady=(10, 0))

        ## Extreme values - Geography type
        self.evs_geo_type_frame = tk.Frame(master=self.middle_frame)

        self.evs_geo_type_label = tk.Label(master=self.evs_geo_type_frame, text='Geography type')
        self.evs_geo_type_label.pack(side=tk.LEFT, padx=(0, 5), pady=5)

        self.evs_geo_type_combobox = ttk.Combobox(master=self.evs_geo_type_frame, state='readonly')
        self.evs_geo_type_combobox['values'] = ['',
                                                'States',
                                                'Metro/micro areas',
                                                'Counties',
                                                'Places',
                                                'Zip codes']
        self.evs_geo_type_combobox.pack(side=tk.LEFT)
        self.evs_geo_type_combobox.bind('<<ComboboxSelected>>', self.handle_within_geo)

        self.evs_geo_type_frame.pack(fill=tk.X, expand=tk.YES)

        ## Extreme values - Within geography
        self.evs_within_geo_frame = tk.Frame(master=self.middle_frame)
        self.evs_within_geo_frame.pack(fill=tk.X, expand=tk.YES)

        ## Extreme values - Filter by
        self.evs_filter_frame = tk.Frame(master=self.middle_frame)

        self.evs_filter_label = tk.Label(master=self.evs_filter_frame, text='Filter by')
        self.evs_filter_label.pack(side=tk.LEFT, padx=(0, 5), pady=5)

        comps = list(self.dp_fetch_one.rl.values())
        comps.insert(0, '')

        self.evs_filter_comp_combobox = ttk.Combobox(master=self.evs_filter_frame, state='readonly')
        self.evs_filter_comp_combobox['values'] = comps
        self.evs_filter_comp_combobox.pack(side=tk.LEFT)

        self.evs_filter_op_dict = {
            'gt': '>',
            'gteq': '>=',
            'eq': '=',
            'lteq': '<=',
            'lt': '<'
        }

        self.evs_filter_op_display_list = list(self.evs_filter_op_dict.values())
        self.evs_filter_op_display_list.insert(0, '')

        self.evs_filter_op_combobox = ttk.Combobox(master=self.evs_filter_frame, width=10, state='readonly')
        self.evs_filter_op_combobox['values'] = self.evs_filter_op_display_list
        self.evs_filter_op_combobox.pack(side=tk.LEFT)

        self.evs_filter_entry = tk.Entry(master=self.evs_filter_frame)
        self.evs_filter_entry.pack(side=tk.LEFT)

        self.evs_filter_frame.pack(fill=tk.X, expand=tk.YES, pady=(0, 10))

        self.middle_frame.pack(fill=tk.X, expand=tk.YES)
        
    def search_key(self, event=None):
        '''Triggered by <Key>, before sending the event to the widget'''
        # If the key pressed isn't a special key, remove placeholder and
        # unitalicize widget text
        if self.search_entry_placeholder == True and event.char != "":
            self.search_entry.delete(0, tk.END)
            self.search_entry.configure(font='Arial 9')
            self.search_entry_placeholder = False

    def search_keyrelease(self, event=None):
        '''Triggered by <KeyRelease>, before sending the event to the widget'''
        # If widget is empty, italicize font, then insert placeholder
        if self.search_entry_placeholder == False and len(self.search_entry.get()) == 0:
            self.search_entry.configure(font='Arial 9 italic')
            self.search_entry.insert(0, 'Search for a geography')
            self.search_entry_placeholder = True
            self.search_entry.icursor(0)

    def search_focusin(self, event=None):
        '''Triggered by placing focus on the widget'''
        if self.search_entry_placeholder == True:
            self.search_entry.icursor(0)

    def search(self):
        self.display_search_results(self.search_entry.get())

    def display_search_results(self, display_label):
        search_window = tk.Toplevel(master=self.root)
        search_window.minsize(500, 150)
        search_window.title(display_label + ' - Search results')
        search_window.columnconfigure(2, weight=1)

        now_loading = tk.Label(master=search_window, text='Now loading. Please wait.')
        now_loading.grid(row=0, column=0, sticky='nsew')
        self.root.update()

        search_results = self.engine.display_label_search(display_label)

        if len(search_results) == 0:
            search_window.destroy()
            tk_messagebox.showinfo('', 'Sorry, there were no results.')
        else:
            now_loading.destroy()
            geography_label = tk.Label(master=search_window, text='Geography', padx=10, font=('TkCaptionFont', 15), anchor='w')
            geography_label.grid(row=0, column=1, sticky='nsew')
            population_label = tk.Label(master=search_window, text='Population', padx=10, font=('TkCaptionFont', 15), anchor='w')
            population_label.grid(row=0, column=2, sticky='nsew')

            for row, search_result in enumerate(search_results[:10]):
                offset = 1
                this_dp_command = partial(self.display_demographic_profile, search_result.name)
                dp_button = tk.Button(master=search_window, text='DP', command=this_dp_command)
                dp_button.grid(row=row + offset, column=0, sticky='nsew')
                geo = tk.Label(master=search_window, text=search_result.name, padx=10, anchor='w')
                geo.grid(row=row + offset, column=1, sticky='nsew')
                pop = tk.Label(master=search_window, text=search_result.fc['population'], padx=10, anchor='w')
                pop.grid(row=row + offset, column=2, sticky='nsew')

    def dp_go(self, event=None):
        self.display_demographic_profile(self.search_entry.get())

    def display_demographic_profile(self, display_label):
        dp_window = tk.Toplevel(master=self.root)
        dp_window.minsize(500, 150)
        dp_window.title(display_label + ' - DemographicProfile')
        dp_window.columnconfigure(2, weight=1)

        now_loading = tk.Label(master=dp_window, text='Now loading. Please wait.')
        now_loading.grid(row=0, column=0, sticky='nsew')
        self.root.update()

        dp_list = self.engine.get_dp(display_label)

        if len(dp_list) == 0:
            dp_window.destroy()
            tk_messagebox.showinfo('', 'Sorry, there is no geography with that name.')
        else:
            now_loading.destroy()
            dp = dp_list[0]

            place = tk.Label(master=dp_window, text=dp.name, padx=10, font=('TkCaptionFont', 20), anchor='w')
            place.grid(row=0, column=2, columnspan=3, sticky='nsew')

            if dp.sumlevel == '160':
                counties_text = ', '.join(dp.counties_display)
                counties = tk.Label(master=dp_window, text=counties_text, padx=10, anchor='w')
                counties.grid(row=1, column=2, columnspan=3, sticky='nsew')

            self.dp_offset = 2
            last_row = 0

            keys = list(self.dp_fetch_one.rl.keys())

            # Currently, DemographicProfiles don't display the following.
            keys.remove('latitude')
            keys.remove('longitude')

            # Move land_area to front of list
            keys.remove('land_area')
            keys.insert(0, 'land_area')

            for row, key in enumerate(keys):
                # Create headers
                if key == 'land_area':
                    self.demographic_profile_header(dp_window, 'Geography', row + self.dp_offset)
                elif key == 'population':
                    self.demographic_profile_header(dp_window, 'Population', row + self.dp_offset)
                elif key == 'white_alone':
                    self.demographic_profile_subheader(dp_window, 'Race', row + self.dp_offset)
                elif key == 'hispanic_or_latino':
                    self.demographic_profile_subheader(dp_window, 'Hispanic or Latino (of any race)', row + self.dp_offset)
                elif key == 'population_25_years_and_older':
                    self.demographic_profile_header(dp_window, 'Education', row + self.dp_offset)
                elif key == 'per_capita_income':
                    self.demographic_profile_header(dp_window, 'Income', row + self.dp_offset)
                elif key == 'median_year_structure_built':
                    self.demographic_profile_header(dp_window, 'Housing', row + self.dp_offset)

                # Special row for population_density (with a formatted compound)
                if key == 'population_density':
                    self.demographic_profile_ev_buttons(master=dp_window, row=row + self.dp_offset, key=key)

                    row_header = tk.Label(master=dp_window, text=dp.rh[key], padx=10, anchor='w')
                    row_header.grid(row=row + self.dp_offset, column=2, sticky='nsew')

                    component = tk.Label(master=dp_window, padx=10, anchor='e')
                    component.grid(row=row + self.dp_offset, column=3, sticky='nsew')

                    compound = tk.Label(master=dp_window, text=dp.fcd[key], padx=10, anchor='e')
                    compound.grid(row=row + self.dp_offset, column=4, sticky='nsew')
                # No compounds for these comps
                elif key in ['land_area', 'population', 'per_capita_income', 'median_household_income',
                             'median_year_structure_built', 'median_rooms', 'median_value', 'median_rent']:
                    self.demographic_profile_row_nc(dp_window, dp, key, row + self.dp_offset)
                # Otherwise, render a standard row.
                else:
                    self.demographic_profile_row_std(dp_window, dp, key, row + self.dp_offset)

                last_row = row + self.dp_offset

            bottom_buttons_frame = tk.Frame(master=dp_window)
            show_cgs_cmd = partial(self.show_closest_geographies, dp.name)
            cgs_button = tk.Button(master=bottom_buttons_frame, text='Show closest geographies', padx=10, command=show_cgs_cmd)
            cgs_button.pack(side=tk.LEFT)
            show_geovectors_cmd = partial(self.show_geovectors, dp.name)
            geovectors_button = tk.Button(master=bottom_buttons_frame, text='Show GeoVectors', padx=10, command=show_geovectors_cmd)
            geovectors_button.pack(side=tk.LEFT)
            bottom_buttons_frame.grid(row=last_row + 1, column=0, columnspan=5, pady=10)

    def demographic_profile_row_std(self, master, dp, key, row):
        '''Render a standard DemographicProfile row'''
        self.demographic_profile_ev_buttons(master, row, key)

        row_header = tk.Label(master=master, text=dp.rh[key], padx=10, anchor='w')
        row_header.grid(row=row, column=2, sticky='nsew')

        component = tk.Label(master=master, text=dp.fc[key], padx=10, anchor='e')
        component.grid(row=row, column=3, sticky='nsew')

        compound = tk.Label(master=master, text=dp.fcd[key], padx=10, anchor='e')
        compound.grid(row=row, column=4, sticky='nsew')

    def demographic_profile_row_nc(self, master, dp, key, row):
        '''Render a DemographicProfile row with no compound'''
        self.demographic_profile_ev_buttons(master, row, key)

        row_header = tk.Label(master=master, text=dp.rh[key], padx=10, anchor='w')
        row_header.grid(row=row, column=2, sticky='nsew')

        component = tk.Label(master=master, padx=10, anchor='e')
        component.grid(row=row, column=3, sticky='nsew')

        compound = tk.Label(master=master, text=dp.fc[key], padx=10, anchor='e')
        compound.grid(row=row, column=4, sticky='nsew')

    def demographic_profile_header(self, master, text, row):
        header = tk.Label(master=master, text=text, padx=10, font=('TkCaptionFont', 15), anchor='w')
        header.grid(row=row, column=2, columnspan=3, sticky='nsew')
        self.dp_offset += 1

    def demographic_profile_subheader(self, master, text, row):
        header = tk.Label(master=master, text=text, padx=10, font=('TkCaptionFont', 12), anchor='w')
        header.grid(row=row, column=2, columnspan=3, sticky='nsew')
        self.dp_offset += 1

    def demographic_profile_ev_buttons(self, master, row, key):
        this_hv_command = partial(self.display_extreme_values, key)
        hv_button = tk.Button(master=master, text='HV', command=this_hv_command)
        hv_button.grid(row=row, column=0, sticky='nsew')

        this_lv_command = partial(self.display_extreme_values, key, lowest=True)
        lv_button = tk.Button(master=master, text='LV', command=this_lv_command)
        lv_button.grid(row=row, column=1, sticky='nsew')

    def show_closest_geographies(self, display_label, event=None):
        cg_window = tk.Toplevel(master=self.root)
        cg_window.minsize(500, 150)
        cg_window.columnconfigure(2, weight=1)
        cg_window.title(display_label + ' - Closest geographies')

        now_loading = tk.Label(master=cg_window, text='Now loading. Please wait.')
        now_loading.grid(row=0, column=0, sticky='nsew')
        self.root.update()

        cgs = self.engine.closest_geographies(display_label, context='p+')

        if len(cgs) == 0:
            cg_window.destroy()
            tk_messagebox.showinfo('', 'Sorry, no geographies can be found.')
        else:
            now_loading.destroy()
            geography_label = tk.Label(master=cg_window, text='Geography', padx=10, font=('TkCaptionFont', 15), anchor='w')
            geography_label.grid(row=0, column=1, sticky='nsew')
            distance_label = tk.Label(master=cg_window, text='Distance (mi)', padx=10, font=('TkCaptionFont', 15), anchor='w')
            distance_label.grid(row=0, column=2, sticky='nsew')

            row_offset = 1

            for row, cg in enumerate(cgs):
                dpi, distance = cg
                this_dp_command = partial(self.display_demographic_profile, dpi.name)
                dp_button = tk.Button(master=cg_window, text='DP', command=this_dp_command)
                dp_button.grid(row=row + row_offset, column=0, sticky='nsew')
                label = tk.Label(master=cg_window, text=dpi.name, padx=10, anchor='w')
                label.grid(row=row + row_offset, column=1, sticky='nsew')
                label = tk.Label(master=cg_window, text=distance, padx=10, anchor='w')
                label.grid(row=row + row_offset, column=2, sticky='nsew')

    def show_geovectors(self, display_label, event=None):
        gv_window = tk.Toplevel(master=self.root)
        gv_window.minsize(500, 150)
        gv_window.columnconfigure(2, weight=1)
        gv_window.title(display_label + ' - Closest GeoVectors')

        now_loading = tk.Label(master=gv_window, text='Now loading. Please wait.')
        now_loading.grid(row=0, column=0, sticky='nsew')
        self.root.update()

        gvs = self.engine.compare_geovectors(display_label)

        if len(gvs) == 0:
            gv_window.destroy()
            tk_messagebox.showinfo('', 'Sorry, no GeoVectors can be found.')
        else:
            now_loading.destroy()
            geography_label = tk.Label(master=gv_window, text='Geography', padx=10, font=('TkCaptionFont', 15), anchor='w')
            geography_label.grid(row=0, column=1, sticky='nsew')

            ## Headers
            offset = 2
            headers = ['PDN', 'PCI', 'WHT', 'BLK', 'ASN', 'HPL', 'BDH', 'GDH', 'Distance']
            for index, header in enumerate(headers):
                label = tk.Label(master=gv_window, text=header, padx=10, font=('TkCaptionFont', 15), anchor='w')
                label.grid(row=0, column=index + offset, sticky='nsew')

            row_offset = 1
            col_offset = 2

            for row, gv in enumerate(gvs[:10]):
                this_dp_command = partial(self.display_demographic_profile, gv.name)
                dp_button = tk.Button(master=gv_window, text='DP', command=this_dp_command)
                dp_button.grid(row=row + row_offset, column=0, sticky='nsew')
                label = tk.Label(master=gv_window, text=gv.name, padx=10, anchor='w')
                label.grid(row=row + row_offset, column=1, sticky='nsew')

                for col, comp in enumerate(gv.ws['std'].keys()):
                    label = tk.Label(master=gv_window, text=str(gv.s[comp]), padx=10, anchor='w')
                    label.grid(row=row + row_offset, column=col + col_offset, sticky='nsew')
                    last_col = col + col_offset

                distance = round(gvs[0].distance(gv, mode='std'), 2)
                label = tk.Label(master=gv_window, text=distance, padx=10, anchor='w')
                label.grid(row=row + row_offset, column=last_col + 1, sticky='nsew')

    def evs_go(self, event=None):
        lowest = self.evs_combobox.get() == 'lowest value'

        # Add the empty string to items from these two dicts
        geofilter_comp_items = list(self.dp_fetch_one.rl.items())
        geofilter_comp_items.insert(0, ('', ''))

        geofilter_op_items = list(self.evs_filter_op_dict.items())
        geofilter_op_items.insert(0, ('', ''))

        # Filters
        geofilter = ''

        geofilter_comp = [i for i, j in geofilter_comp_items if j == self.evs_filter_comp_combobox.get()][0]
        geofilter_op = [i for i, j in geofilter_op_items if j == self.evs_filter_op_combobox.get()][0]
        geofilter_val = self.evs_filter_entry.get()

        geofilter_list = [geofilter_comp, geofilter_op, geofilter_val]

        if len([i for i in geofilter_list if i]) == 3:
            geofilter = ':'.join(geofilter_list)

        # Geography types
        context = ''

        geo_type = self.evs_geo_type_combobox.get()

        geo_type_dict = {
            'States': 's+',
            'Metro/micro areas': 'cb+',
            'Counties': 'c+',
            'Places': 'p+',
            'Zip codes': 'z+'
        }

        if geo_type != '':
            geo_type = geo_type_dict[geo_type]

        context = geo_type

        comp = [i for i, j in self.dp_fetch_one.rl.items() if j == self.evs_comp_combobox.get()][0]

        ## Get geographical context, if there is one
        # ZCTAs (Zip codes)
        if self.test_removable_widget_existance(self.evs_zcta_entry):
            zcta_group = self.evs_zcta_entry.get()
            context += zcta_group
        # States + Counties
        elif self.test_removable_widget_existance(self.evs_within_county_combobox):
            county_name = self.evs_within_county_combobox.get()
            if county_name != 'All counties':
                county_key = self.engine.kt.county_name_to_key[county_name]
                context += county_key[3:].replace('/county', '')
            else:
                context += self.get_state_key()
        # States
        elif self.test_removable_widget_existance(self.evs_within_state_combobox):
            context += self.get_state_key()

        self.display_extreme_values(comp, context=context, geofilter=geofilter, lowest=lowest)

    def get_state_key(self):
        state_name = self.evs_within_state_combobox.get()
        if state_name != 'All states':
            state_key = self.engine.st.get_abbrev(state_name, lowercase=True)
        else:
            state_key = ''

        return state_key

    def display_extreme_values(self, comp, context='', geofilter='', lowest=False):
        val_window = tk.Toplevel(master=self.root)
        val_window.minsize(500, 150)
        val_window.title('Extreme values')
        val_window.columnconfigure(1, weight=1)

        now_loading = tk.Label(master=val_window, text='Now loading. Please wait.')
        now_loading.grid(row=0, column=0, columnspan=2, sticky='nsew')
        self.root.update()

        data_type = 'c'

        if comp in ['population_density',
                    'white_alone',
                    'white_alone_not_hispanic_or_latino',
                    'black_alone',
                    'asian_alone',
                    'other_race',
                    'hispanic_or_latino',
                    'population_25_years_and_older',
                    'bachelors_degree_or_higher',
                    'graduate_degree_or_higher']:
            data_type = 'cc'

        evs = self.engine.extreme_values(comp, context=context, geofilter=geofilter, data_type=data_type, lowest=lowest)

        if len(evs) == 0:
            val_window.destroy()
            tk_messagebox.showinfo('', 'Sorry, there are no geographies to display.')
        else:
            now_loading.destroy()
            fetch_one = evs[0]

            geography_label = tk.Label(master=val_window, text='Geography', padx=10, font=('TkCaptionFont', 15), anchor='w')
            geography_label.grid(row=0, column=1, sticky='nsew')
            population_label = tk.Label(master=val_window, text='Population', padx=10, font=('TkCaptionFont', 15), anchor='w')
            population_label.grid(row=0, column=2, sticky='nsew')

            if comp != 'population':
                comp_label = tk.Label(master=val_window, text=fetch_one.rl[comp], padx=10, font=('TkCaptionFont', 15), anchor='w')
                comp_label.grid(row=0, column=3, sticky='nsew')

            for row, ev in enumerate(evs[:10]):
                offset = 1
                this_dp_command = partial(self.display_demographic_profile, ev.name)
                dp_button = tk.Button(master=val_window, text='DP', command=this_dp_command)
                dp_button.grid(row=row + offset, column=0, sticky='nsew')
                geo = tk.Label(master=val_window, text=ev.name, padx=10, anchor='w')
                geo.grid(row=row + offset, column=1, sticky='nsew')
                pop = tk.Label(master=val_window, text=ev.fc['population'], padx=10, anchor='w')
                pop.grid(row=row + offset, column=2, sticky='nsew')

                if comp != 'population':
                    if comp == 'population_density':
                        data = ev.fcd[comp]
                    else:
                        data = ev.fc[comp]

                    if data_type == 'cc':
                        data = ev.fcd[comp]

                    data = tk.Label(master=val_window, text=data, padx=10, anchor='w')
                    data.grid(row=row + offset, column=3, sticky='nsew')

    def handle_within_geo(self, event=None):
        geo_type = self.evs_geo_type_combobox.get()

        self.destroy_removable_widgets_if_exists()

        if geo_type == 'Places' or geo_type == 'Counties':
            self.evs_within_geo_label = tk.Label(master=self.evs_within_geo_frame, text='Within', anchor='w')
            self.evs_within_geo_label.pack(side=tk.LEFT, padx=(0, 5), pady=5)

            self.evs_within_state_combobox = ttk.Combobox(master=self.evs_within_geo_frame, state='readonly')
            self.evs_within_state_combobox['values'] = ['All states',
                                                        'Alaska',
                                                        'Alabama',
                                                        'Arkansas',
                                                        'Arizona',
                                                        'California',
                                                        'Colorado',
                                                        'Connecticut',
                                                        'District of Columbia',
                                                        'Delaware',
                                                        'Florida',
                                                        'Georgia',
                                                        'Guam',
                                                        'Hawaii',
                                                        'Iowa',
                                                        'Idaho',
                                                        'Illinois',
                                                        'Indiana',
                                                        'Kansas',
                                                        'Kentucky',
                                                        'Louisiana',
                                                        'Massachusetts',
                                                        'Maryland',
                                                        'Maine',
                                                        'Michigan',
                                                        'Minnesota',
                                                        'Missouri',
                                                        'Mississippi',
                                                        'Montana',
                                                        'National',
                                                        'North Carolina',
                                                        'North Dakota',
                                                        'Nebraska',
                                                        'New Hampshire',
                                                        'New Jersey',
                                                        'New Mexico',
                                                        'Nevada',
                                                        'New York',
                                                        'Ohio',
                                                        'Oklahoma',
                                                        'Oregon',
                                                        'Pennsylvania',
                                                        'Puerto Rico',
                                                        'Rhode Island',
                                                        'South Carolina',
                                                        'South Dakota',
                                                        'Tennessee',
                                                        'Texas',
                                                        'Utah',
                                                        'Virginia',
                                                        'Virgin Islands',
                                                        'Vermont',
                                                        'Washington',
                                                        'Wisconsin',
                                                        'West Virginia',
                                                        'Wyoming']
            self.evs_within_state_combobox.pack(side=tk.LEFT)
            self.evs_within_state_combobox.set('All states')
            if geo_type == 'Places':
                self.evs_within_state_combobox.bind('<<ComboboxSelected>>', self.display_counties)
        elif geo_type == 'Zip codes':
            self.evs_within_geo_label = tk.Label(master=self.evs_within_geo_frame, text='Starts with', anchor='w')
            self.evs_within_geo_label.pack(side=tk.LEFT, padx=(0, 5), pady=5)

            self.evs_zcta_entry = tk.Entry(master=self.evs_within_geo_frame)
            self.evs_zcta_entry.pack(side=tk.LEFT)

    def display_counties(self, event=None):
        state = self.evs_within_state_combobox.get()

        self.destroy_evs_within_county_combobox_if_exists()

        if state != 'All states':
            state_geoid = self.engine.st.name_to_geoid[state]

            all_counties = dict(self.engine.ct.county_geoid_to_name)
            target_counties = []
            
            for key, value in all_counties.items():
                if key.startswith(state_geoid):
                    target_counties.append(value)

            target_counties.insert(0, 'All counties')

            self.evs_within_county_combobox = ttk.Combobox(master=self.evs_within_geo_frame, state='readonly')
            self.evs_within_county_combobox['values'] = target_counties
            self.evs_within_county_combobox.pack(side=tk.LEFT)
            self.evs_within_county_combobox.set('All counties')
        else:
            self.evs_within_county_combobox.forget()

    def destroy_evs_within_county_combobox_if_exists(self):
        if self.evs_within_county_combobox and self.evs_within_county_combobox.winfo_exists:
            self.evs_within_county_combobox.destroy()

    def destroy_removable_widgets_if_exists(self):
        removable_widgets = [self.evs_within_geo_label,
                             self.evs_within_state_combobox,
                             self.evs_within_county_combobox,
                             self.evs_zcta_entry]

        for removable_widget in removable_widgets:
            if self.test_removable_widget_existance(removable_widget):
                removable_widget.destroy()

    def test_removable_widget_existance(self, widget):
        if widget:
            return widget.winfo_exists()
        else:
            return False

    def activate_mainloop(self):
        self.root.mainloop()

def main():
    gui = GeodataGUI()
    gui.activate_mainloop()


if __name__ == '__main__':
    main()
