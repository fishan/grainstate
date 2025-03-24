import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import Dash, dcc, html, Input, Output
from pathlib import Path
from datetime import timedelta, datetime
import base64
import io

# Пути к файлам
input_dir = Path('final_data')
moistures_file = input_dir / 'moistures_temps_mass.csv'
settings_file = input_dir / 'settings_optimized.csv'
alarms_segments_file = input_dir / 'alarms_segments.csv'

# Читаем данные
df = pd.read_csv(moistures_file)
settings_df = pd.read_csv(settings_file)
alarms_segments = pd.read_csv(alarms_segments_file)

# Преобразуем DateTime и смещаем на +3 часа
df['DateTime'] = pd.to_datetime(df['Date'] + ' ' + df['Time'], format='%d-%m-%Y %H:%M:%S') + timedelta(hours=3)
settings_df['DateTime'] = pd.to_datetime(settings_df['Date'] + ' ' + settings_df['Time'], format='%d-%m-%Y %H:%M:%S') + timedelta(hours=3)
alarms_segments['Start'] = pd.to_datetime(alarms_segments['Start'])
alarms_segments['End'] = pd.to_datetime(alarms_segments['End'])

# Объединяем данные
df = df.merge(settings_df[['DateTime', 'DROPS_SET_TIMER', 'SET_BURNERS_TEMP']], on='DateTime', how='left')

# Классификация влажности
def classify_moisture(row):
    grain_type = row['GRAIN_TYPE']
    moisture = row['perten_dry_Moisture']
    if pd.isna(grain_type) or pd.isna(moisture):
        return 'unknown'
    elif grain_type.lower() == 'raps':
        return 'overdry' if moisture < 8 else 'normal' if 8 <= moisture <= 9.5 else 'wet'
    else:
        return 'overdry' if moisture < 12.9 else 'normal' if 12.9 <= moisture <= 14.5 else 'wet'

df['Moisture_Status'] = df.apply(classify_moisture, axis=1)

# Поступающая влажность и возврат
df['Moisture_Diff'] = df['perten_wet_Moisture'] - df['perten_dry_Moisture']
df['Incoming_Wet'] = df['Moisture_Diff'].apply(lambda x: x if x > 1 else None)
avg_incoming_wet = df[df['Incoming_Wet'].notna()].groupby('GRAIN_TYPE')['perten_wet_Moisture'].agg(['mean', 'min']).reset_index()
returned_mass = df[df['Moisture_Diff'].notna() & (df['Moisture_Diff'] <= 1)]['dry_mass'].sum()

# Определение смены (день: 8:00-20:00, ночь: 20:00-8:00)
def get_shift(datetime_obj):
    hour = datetime_obj.hour
    date = datetime_obj.date()
    if 8 <= hour < 20:
        shift_start = datetime(date.year, date.month, date.day, 8, 0)
        return shift_start, 'Day'
    else:
        if hour < 8:
            shift_start = datetime(date.year, date.month, date.day, 0, 0) - timedelta(hours=4)  # 20:00 предыдущего дня
        else:
            shift_start = datetime(date.year, date.month, date.day, 20, 0)
        return shift_start, 'Night'

# Добавляем информацию о сменах и считаем массу по сменам
df['Shift_Start'], df['Shift_Type'] = zip(*df['DateTime'].apply(get_shift))
shift_productivity = df.groupby(['Shift_Start', 'Shift_Type'])['dry_mass'].sum().reset_index()
shift_productivity['Shift_Start'] = pd.to_datetime(shift_productivity['Shift_Start'])

# Время работы режимов
df['Time_Diff'] = df['DateTime'].shift(-1) - df['DateTime']
df['Time_Diff'] = df['Time_Diff'].fillna(timedelta(seconds=0)).dt.total_seconds() / 3600  # В часы
mode_times = df.groupby('mode')['Time_Diff'].sum().reset_index()
total_work_time = mode_times[mode_times['mode'] != 'STOP']['Time_Diff'].sum()

# Сушёное зерно: масса и влажность
dry_mass_stats = df.groupby('GRAIN_TYPE').agg({'dry_mass': 'sum', 'perten_dry_Moisture': ['min', 'mean']}).reset_index()
dry_mass_stats.columns = ['GRAIN_TYPE', 'Total_Dry_Mass', 'Min_Dry_Moisture', 'Mean_Dry_Moisture']
moisture_by_grain = df.groupby(['GRAIN_TYPE', 'Moisture_Status'])['dry_mass'].sum().reset_index()

# Корреляции
grain_types = df['GRAIN_TYPE'].dropna().unique()
correlations = {}
for grain in grain_types:
    grain_df = df[df['GRAIN_TYPE'] == grain].dropna(subset=['SET_BURNERS_TEMP', 'DROPS_SET_TIMER', 'ACTUAL_BURNERS_TEMP', 'perten_dry_Moisture', 'dry_mass'])
    if len(grain_df) < 2 or grain_df['SET_BURNERS_TEMP'].std() == 0 or grain_df['DROPS_SET_TIMER'].std() == 0:
        correlations[grain] = {k: float('nan') for k in ['temp_moisture', 'timer_moisture', 'actual_temp_moisture', 'timer_mass', 'temp_mass']}
    else:
        correlations[grain] = {
            'temp_moisture': grain_df['SET_BURNERS_TEMP'].corr(grain_df['perten_dry_Moisture']),
            'timer_moisture': grain_df['DROPS_SET_TIMER'].corr(grain_df['perten_dry_Moisture']),
            'actual_temp_moisture': grain_df['ACTUAL_BURNERS_TEMP'].corr(grain_df['perten_dry_Moisture']),
            'timer_mass': grain_df['DROPS_SET_TIMER'].corr(grain_df['dry_mass']),
            'temp_mass': grain_df['SET_BURNERS_TEMP'].corr(grain_df['dry_mass'])
        }

# Тексты на двух языках
texts = {
    'en': {
        'title': "Grainstate: Grain Drying Analytics",
        'intro': "This report provides detailed analytics for grain drying performance over the period {} - {} (Local Time, UTC+3), collected using Perten AM5200A and Grainstate systems. Select a grain type below to filter data and receive operator recommendations based on statistical analysis of moisture, mass, and alarm trends.",
        'summary': "Key Results",
        'total_mass': "Total dried mass: {:.0f} kg",
        'drops': "Number of drops: {}",
        'returned': "Returned wet grain: {:.0f} kg",
        'wet_moisture': "Incoming moisture by grain type (wet > dry by 1%):",
        'grain_filter': "Grain type:",
        'mode_pie': "Operating Modes (hours)",
        'mass_pie': "Dried Mass by Grain Type (%)",
        'moisture_bar': "Dried Mass by Moisture Status (kg)",
        'shift_line': "Dried Mass by Shift (kg, Day: 8:00-20:00, Night: 20:00-8:00)",
        'dry_moisture_bar': "Dry Moisture by Grain Type (%)",
        'settings_scatter': "Set Temperature vs Dry Moisture",
        'alarms_timeline': "Dryer Alarms Timeline (Segments)",
        'operator_notes': "Operator Recommendations",
        'notes_intro': "Statistics-based guidelines for optimal drying:",
        'notes': {
            'temp_moisture': "Set Temperature vs Dry Moisture: {:.2f}. Higher temperature dries better if negative (e.g., -0.5 = strong drying), wetter grain if positive (e.g., 0.3 = less drying). NaN = insufficient data.",
            'timer_moisture': "Drop Interval vs Dry Moisture: {:.2f}. Longer intervals dry better if negative (e.g., -0.4 = good drying), shorter intervals leave wetter grain if positive (e.g., 0.2).",
            'actual_temp_moisture': "Actual Temperature vs Dry Moisture: {:.2f}. Hotter burns dry better if negative (e.g., -0.6 = strong effect), less drying if positive.",
            'timer_mass': "Drop Interval vs Dried Mass: {:.2f}. Shorter intervals increase mass if negative (e.g., -0.3), longer intervals increase mass if positive.",
            'temp_mass': "Set Temperature vs Dried Mass: {:.2f}. Higher temperature increases mass if positive (e.g., 0.4), less mass if negative.",
            'general': [
                "Frequent alarm segments (see timeline) indicate overloading—reduce input rate.",
                "If incoming moisture is close to dry (<1% difference), increase SET_BURNERS_TEMP by 5-10°C to avoid returns.",
                "Dryer worked {:.1f} hours (excluding STOP mode). Adjust input rate if wet grain exceeds 20% per grain type.",
                "For Raps: overdry < 8%, normal 8-9.5%. For others: overdry < 12.9%, normal 12.9-14.5%."
            ]
        },
        'conclusion': "Why Grainstate?",
        'conclusion_text': "Real-time drying optimization. Contact us for a demo: info@grainstate.com",
        'download': "Download data for {} (CSV)"
    },
    'et': {
        'title': "Grainstate: Teravilja kuivatamise analüütika",
        'intro': "See aruanne annab üksikasjaliku ülevaate teravilja kuivatamise tulemustest perioodil {} - {} (Kohalik aeg, UTC+3), kogutud Perten AM5200A ja Grainstate süsteemide abil. Vali allpool teravilja tüüp, et filtreerida andmeid ja saada operaatori soovitusi, mis põhinevad niiskuse, massi ja häirete statistilisel analüüsil.",
        'summary': "Peamised tulemused",
        'total_mass': "Kuivatatud teravilja mass: {:.0f} kg",
        'drops': "Tühjendamiste arv: {}",
        'returned': "Tagastatud märg teravili: {:.0f} kg",
        'wet_moisture': "Sissetulev niiskus teravilja tüübi järgi (märg > kuiv 1% võrra):",
        'grain_filter': "Teravilja tüüp:",
        'mode_pie': "Töörežiimid (tundi)",
        'mass_pie': "Kuivatatud mass teravilja tüübi järgi (%)",
        'moisture_bar': "Kuivatatud mass niiskuse staatuse järgi (kg)",
        'shift_line': "Kuivatatud mass vahetuste järgi (kg, Päev: 8:00-20:00, Öö: 20:00-8:00)",
        'dry_moisture_bar': "Kuiva niiskus teravilja tüübi järgi (%)",
        'settings_scatter': "Määratud temperatuur vs Kuiv niiskus",
        'alarms_timeline': "Kuivati häirete ajajoon (segmendid)",
        'operator_notes': "Operaatori soovitused",
        'notes_intro': "Statistikapõhised juhised optimaalseks kuivatamiseks:",
        'notes': {
            'temp_moisture': "Määratud temperatuur vs Kuiv niiskus: {:.2f}. Kõrgem temperatuur kuivatab paremini, kui negatiivне (nt -0.5 = tugev kuivatamine), märjem teravili, kui positiivне (nt 0.3 = vähem kuivatamist). NaN = puuduvad andmed.",
            'timer_moisture': "Tühjendusintervall vs Kuiv niiskus: {:.2f}. Pikem intervall kuivatab paremini, kui negатиивне (nt -0.4 = hea kuivatamine), lühem jätab märjemaks, kui positiивне (nt 0.2).",
            'actual_temp_moisture': "Tegelik temperatuur vs Kuiv niiskus: {:.2f}. Kuumem kuivatab paremini, kui negатиивне (nt -0.6 = tugev mõju), vähem kuivatamist, kui positiивне.",
            'timer_mass': "Tühjendusintervall vs Kuivatatud mass: {:.2f}. Lühem intervall suurendab massи, kui negатиивне (nt -0.3), pikem suurendab, kui positiивне.",
            'temp_mass': "Määratud temperatuur vs Kuivatatud mass: {:.2f}. Kõrgem temperatuur suurendab massи, kui positiивне (nt 0.4), vähem massи, kui negатиивне.",
            'general': [
                "Sagedased häiresegmendid (vaata ajajoont) viitavad ülekoormusele—vähenda sisendkiirust.",
                "Kui märg niiskus on kuivale lähedal (<1% vahe), tõsta SET_BURNERS_TEMP 5-10°C võrra, et vältida tagastamist.",
                "Kuivati töötas {:.1f} tundi (v.a STOP režiim). Kohanda sisendkiirust, kui märg teravili ületab 20% teravilja tüübi kohta.",
                "Rapsi jaoks: ülekuiv < 8%, normaalne 8-9.5%. Teiste jaoks: ülekuiv < 12.9%, normaalne 12.9-14.5%."
            ]
        },
        'conclusion': "Miks Grainstate?",
        'conclusion_text': "Reaalajas kuivatamise optimeerimine. Võtke ühendust demo jaoks: info@grainstate.com",
        'download': "Laadi alla andmed {} jaoks (CSV)"
    }
}

# Функция для создания ссылки на скачивание CSV
def create_download_link(df, filename, title):
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_string = csv_buffer.getvalue()
    b64 = base64.b64encode(csv_string.encode()).decode()
    return html.A(title, href=f"data:text/csv;base64,{b64}", download=filename, style={'color': '#2980b9', 'textDecoration': 'underline'})

# Инициализируем Dash
app = Dash(__name__)

# Стили
styles = {
    'container': {'fontFamily': 'Arial', 'padding': '20px', 'backgroundColor': '#f9f9f9', 'maxWidth': '1200px', 'margin': '0 auto'},
    'header': {'color': '#2c3e50', 'textAlign': 'center', 'fontSize': '32px', 'marginBottom': '20px'},
    'text': {'color': '#34495e', 'fontSize': '16px', 'lineHeight': '1.5'},
    'graph': {'margin': '20px 0', 'border': '1px solid #ddd', 'borderRadius': '5px', 'backgroundColor': '#fff'}
}

# Макет
app.layout = html.Div([
    html.H1(id='title', style=styles['header']),
    html.P(id='intro', style=styles['text']),
    
    dcc.Dropdown(
        id='language',
        options=[{'label': 'English', 'value': 'en'}, {'label': 'Eesti keel', 'value': 'et'}],
        value='en',
        style={'width': '40%', 'margin': '10px auto', 'padding': '5px', 'fontSize': '20px'},
        clearable=False,
        placeholder="Select Language"
    ),

    html.Label(id='grain_filter_label', style=styles['text']),
    dcc.Dropdown(
        id='grain-filter',
        options=[{'label': grain, 'value': grain} for grain in df['GRAIN_TYPE'].dropna().unique() if grain],
        value=None,
        multi=False,
        style={'width': '50%', 'margin': '10px auto', 'padding': '5px', 'fontSize': '20px'},
        placeholder="Select Grain Type"
    ),

    html.H3(id='operator_notes', style={**styles['text'], 'fontSize': '24px'}),
    html.P(id='notes_intro', style=styles['text']),
    html.Ul(id='notes_list', style=styles['text']),

    html.Div([
        html.H3(id='summary', style={**styles['text'], 'fontSize': '24px'}),
        html.P(id='total_mass', style=styles['text']),
        html.P(id='drops', style=styles['text']),
        html.P(id='returned', style=styles['text']),
        html.P(id='wet_moisture', style=styles['text']),
        dcc.Graph(id='wet_moisture_table'),
        html.Div(id='wet_moisture_data')
    ], style={'backgroundColor': '#ecf0f1', 'padding': '15px', 'borderRadius': '5px'}),

    dcc.Graph(id='mode-pie', style=styles['graph']),
    html.Div(id='mode_data'),
    dcc.Graph(id='mass-pie', style=styles['graph']),
    html.Div(id='mass_data'),
    dcc.Graph(id='moisture-bar', style=styles['graph']),
    html.Div(id='moisture_data'),
    dcc.Graph(id='shift-line', style=styles['graph']),
    html.Div(id='shift_data'),
    dcc.Graph(id='dry-moisture-bar', style=styles['graph']),
    html.Div(id='dry_moisture_data'),
    dcc.Graph(id='settings-scatter', style=styles['graph']),
    html.Div(id='settings_data'),
    html.H3(id='alarms_timeline_title', style={**styles['text'], 'fontSize': '24px'}),
    dcc.Graph(id='alarms_timeline', style=styles['graph']),
    html.Div(id='alarms_data'),

    html.H3(id='conclusion', style={**styles['text'], 'fontSize': '24px'}),
    html.P(id='conclusion_text', style=styles['text'])
], style=styles['container'])

# Callback
@app.callback(
    [Output('title', 'children'),
     Output('intro', 'children'),
     Output('grain_filter_label', 'children'),
     Output('operator_notes', 'children'),
     Output('notes_intro', 'children'),
     Output('notes_list', 'children'),
     Output('summary', 'children'),
     Output('total_mass', 'children'),
     Output('drops', 'children'),
     Output('returned', 'children'),
     Output('wet_moisture', 'children'),
     Output('wet_moisture_table', 'figure'),
     Output('wet_moisture_data', 'children'),
     Output('mode-pie', 'figure'),
     Output('mode_data', 'children'),
     Output('mass-pie', 'figure'),
     Output('mass_data', 'children'),
     Output('moisture-bar', 'figure'),
     Output('moisture_data', 'children'),
     Output('shift-line', 'figure'),
     Output('shift_data', 'children'),
     Output('dry-moisture-bar', 'figure'),
     Output('dry_moisture_data', 'children'),
     Output('settings-scatter', 'figure'),
     Output('settings_data', 'children'),
     Output('alarms_timeline_title', 'children'),
     Output('alarms_timeline', 'figure'),
     Output('alarms_data', 'children'),
     Output('conclusion', 'children'),
     Output('conclusion_text', 'children')],
    [Input('language', 'value'),
     Input('grain-filter', 'value')]
)
def update_report(lang, selected_grain):
    filtered_df = df if selected_grain is None else df[df['GRAIN_TYPE'] == selected_grain]
    filtered_moisture = moisture_by_grain if selected_grain is None else moisture_by_grain[moisture_by_grain['GRAIN_TYPE'] == selected_grain]
    filtered_dry_mass = dry_mass_stats if selected_grain is None else dry_mass_stats[dry_mass_stats['GRAIN_TYPE'] == selected_grain]
    filtered_mode_times = mode_times

    # Корреляции
    corr = correlations.get(selected_grain, {}) if selected_grain else {}
    if not selected_grain:
        all_df = df.dropna(subset=['SET_BURNERS_TEMP', 'DROPS_SET_TIMER', 'ACTUAL_BURNERS_TEMP', 'perten_dry_Moisture', 'dry_mass'])
        if len(all_df) >= 2 and all_df['SET_BURNERS_TEMP'].std() != 0 and all_df['DROPS_SET_TIMER'].std() != 0:
            corr = {
                'temp_moisture': all_df['SET_BURNERS_TEMP'].corr(all_df['perten_dry_Moisture']),
                'timer_moisture': all_df['DROPS_SET_TIMER'].corr(all_df['perten_dry_Moisture']),
                'actual_temp_moisture': all_df['ACTUAL_BURNERS_TEMP'].corr(all_df['perten_dry_Moisture']),
                'timer_mass': all_df['DROPS_SET_TIMER'].corr(all_df['dry_mass']),
                'temp_mass': all_df['SET_BURNERS_TEMP'].corr(all_df['dry_mass'])
            }

    t = texts[lang]
    intro = t['intro'].format(df['DateTime'].min().strftime('%d-%m-%Y'), df['DateTime'].max().strftime('%d-%m-%Y'))
    total_mass = t['total_mass'].format(filtered_df['dry_mass'].sum())
    drops = t['drops'].format(len(filtered_df['DROPS_SCORE'].dropna().unique()))
    returned = t['returned'].format(filtered_df[filtered_df['Moisture_Diff'].notna() & (filtered_df['Moisture_Diff'] <= 1)]['dry_mass'].sum())
    wet_moisture = t['wet_moisture']
    wet_moisture_fig = px.bar(avg_incoming_wet, x='GRAIN_TYPE', y=['mean', 'min'], 
                              title='Incoming Moisture (%)' if lang == 'en' else 'Sissetulev niiskus (%)',
                              barmode='group', text_auto='.1f')
    wet_moisture_fig.update_layout(font={'size': 14}, dragmode='pan', xaxis={'fixedrange': True}, yaxis={'fixedrange': True})
    wet_moisture_data = create_download_link(avg_incoming_wet, 'wet_moisture.csv', t['download'].format('Incoming Moisture'))

    mode_fig = px.pie(filtered_mode_times, names='mode', values='Time_Diff', title=t['mode_pie'], hole=0.4,
                      color_discrete_sequence=px.colors.qualitative.Pastel)
    mode_fig.update_layout(font={'size': 14}, dragmode='pan')
    mode_data = create_download_link(filtered_mode_times, 'mode_times.csv', t['download'].format('Operating Modes'))

    mass_fig = px.pie(filtered_dry_mass, names='GRAIN_TYPE', values='Total_Dry_Mass', title=t['mass_pie'], hole=0.4,
                      color_discrete_sequence=px.colors.qualitative.Plotly)
    mass_fig.update_traces(textinfo='percent+label+value', texttemplate='%{label}: %{value:.0f} kg (%{percent})')
    mass_fig.update_layout(font={'size': 14}, dragmode='pan')
    mass_data = create_download_link(filtered_dry_mass, 'dry_mass_stats.csv', t['download'].format('Dried Mass'))

    moisture_fig = px.bar(filtered_moisture, x='GRAIN_TYPE', y='dry_mass', color='Moisture_Status', 
                          title=t['moisture_bar'], barmode='group', text_auto='.0f',
                          color_discrete_map={'overdry': '#e74c3c', 'normal': '#2ecc71', 'wet': '#3498db'})
    moisture_fig.update_layout(font={'size': 14}, dragmode='pan', xaxis={'fixedrange': True}, yaxis={'fixedrange': True})
    moisture_data = create_download_link(filtered_moisture, 'moisture_by_grain.csv', t['download'].format('Dried Mass by Moisture Status'))

    # График по сменам
    shift_filtered = shift_productivity[shift_productivity['Shift_Start'].isin(filtered_df['Shift_Start'])]
    shift_fig = px.bar(shift_filtered, x='Shift_Start', y='dry_mass', color='Shift_Type',
                       title=t['shift_line'],
                       labels={'dry_mass': 'Mass (kg)', 'Shift_Start': 'Shift Start Time'},
                       text_auto='.0f',
                       color_discrete_map={'Day': '#3498db', 'Night': '#2ecc71'})  # Синий для дня, зеленый для ночи
    shift_fig.update_layout(font={'size': 14}, dragmode='pan', xaxis={'tickangle': -45, 'fixedrange': True}, yaxis={'fixedrange': True})
    shift_data = create_download_link(shift_filtered, 'shift_mass.csv', t['download'].format('Dried Mass by Shift'))

    dry_moisture_fig = px.bar(filtered_dry_mass, x='GRAIN_TYPE', y=['Mean_Dry_Moisture', 'Min_Dry_Moisture'], 
                              title=t['dry_moisture_bar'], barmode='group', text_auto='.1f')
    dry_moisture_fig.update_layout(font={'size': 14}, dragmode='pan', xaxis={'fixedrange': True}, yaxis={'fixedrange': True})
    dry_moisture_data = create_download_link(filtered_dry_mass, 'dry_moisture_stats.csv', t['download'].format('Dry Moisture by Grain Type'))

    scatter_fig = px.scatter(filtered_df, x='SET_BURNERS_TEMP', y='perten_dry_Moisture', size='DROPS_SET_TIMER',
                             title=t['settings_scatter'], color='GRAIN_TYPE',
                             labels={'SET_BURNERS_TEMP': 'Temperature (°C)', 'perten_dry_Moisture': 'Moisture (%)'},
                             color_discrete_sequence=px.colors.qualitative.Plotly)
    scatter_fig.update_layout(font={'size': 14}, dragmode='pan', xaxis={'range': [40, None], 'fixedrange': True}, yaxis={'fixedrange': True})
    settings_data = create_download_link(filtered_df[['DateTime', 'SET_BURNERS_TEMP', 'perten_dry_Moisture', 'DROPS_SET_TIMER', 'GRAIN_TYPE']], 
                                        'settings_data.csv', t['download'].format('Set Temperature vs Dry Moisture'))

    filtered_alarms = alarms_segments if selected_grain is None else alarms_segments[alarms_segments['Start'].isin(filtered_df['DateTime'])]
    if filtered_alarms.empty:
        alarms_timeline_fig = go.Figure()
        alarms_timeline_fig.update_layout(title=t['alarms_timeline'], xaxis_title="Time", yaxis_title="Alarm Type", yaxis={'tickmode': 'array', 'tickvals': []})
    else:
        alarms_timeline_fig = go.Figure()
        alarm_types = filtered_alarms['Alarm_Type'].unique()
        colors = px.colors.qualitative.Plotly[:len(alarm_types)]
        for i, alarm_type in enumerate(alarm_types):
            alarm_data = filtered_alarms[filtered_alarms['Alarm_Type'] == alarm_type]
            for _, row in alarm_data.iterrows():
                alarms_timeline_fig.add_trace(go.Scatter(
                    x=[row['Start'], row['End']],
                    y=[i, i],
                    mode='lines',
                    line=dict(width=10, color=colors[i]),
                    name=alarm_type,
                    showlegend=True if row.name == alarm_data.index[0] else False
                ))
        alarms_timeline_fig.update_layout(
            title=t['alarms_timeline'], xaxis_title="Time", yaxis_title="Alarm Type",
            yaxis={'tickmode': 'array', 'tickvals': list(range(len(alarm_types))), 'ticktext': alarm_types, 'fixedrange': True},
            font={'size': 14}, dragmode='pan', showlegend=True, height=400
        )
    alarms_data = create_download_link(filtered_alarms, 'alarms_segments.csv', t['download'].format('Dryer Alarms'))

    # Рекомендации
    notes_list = []
    if selected_grain and corr:
        for key, value in corr.items():
            if pd.isna(value):
                notes_list.append(html.Li(f"{t['notes'][key].split(':')[0]}: Not enough data or no variation for {selected_grain}."))
            else:
                notes_list.append(html.Li(t['notes'][key].format(value)))
    notes_list.extend([html.Li(t['notes']['general'][i].format(total_work_time) if i == 2 else t['notes']['general'][i]) for i in range(len(t['notes']['general']))])

    return (t['title'], intro, t['grain_filter'], t['operator_notes'], t['notes_intro'], notes_list,
            t['summary'], total_mass, drops, returned, wet_moisture, wet_moisture_fig, wet_moisture_data,
            mode_fig, mode_data, mass_fig, mass_data, moisture_fig, moisture_data, shift_fig, shift_data,
            dry_moisture_fig, dry_moisture_data, scatter_fig, settings_data, t['alarms_timeline'], alarms_timeline_fig, alarms_data,
            t['conclusion'], t['conclusion_text'])

# Запуск и экспорт
if __name__ == '__main__':
    app.run(debug=True)
    with open('index.html', 'w') as f:
        f.write(app.index())