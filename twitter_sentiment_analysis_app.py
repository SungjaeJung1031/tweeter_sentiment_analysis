import dash
from dash.dependencies import Output, Input
import dash_core_components as dcc
import dash_html_components as html
import plotly
import plotly.graph_objects as go
import random
from collections import deque
import sqlite3
import pandas as pd
from cache import cache
import time

# popular topics: google, olympics, trump, gun, usa

color_palette = {
    'orange': '#E95420',
    'white': '#FFFFFF',
    'black': '#000000',
    'aubergine_light': '#77216F',
    'aubergine_mid': '#5E2750',
    'aubergine_dark': '#2C001E',
    'aubergine_canonical': '#772953',
    'grey_warm': '#AEA79F',
    'grey_cool': '#333333',
    'grey_text': '#111111',
    'red': '#FF0000',
    'blue': '#0000FF'
}


app = dash.Dash(__name__)
app.layout = html.Div(
    [html.Div(className='container-fluid', children=[html.H2('Live Twitter Sentiment', style={'color': color_palette['black']}),
                                                     html.H4('Search:', style={
                                                         'color': color_palette['black']}),
                                                     dcc.Input(id='sentiment_term', value='twitter', type='text', style={
                                                               'color': color_palette['black']}),
                                                     ],
              style={'width': '98%', 'margin-top': 50, 'margin-left': 10, 'margin-right': 10, 'max-width': 50000}),

        # html.Div(className='row', children=[html.Div(id='related-sentiment', children=html.Button('Loading related terms...', id='related_term_button'), className='col s12 m6 l6', style={"word-wrap": "break-word"}),
        #                                     html.Div(id='recent-trending', className='col s12 m6 l6', style={"word-wrap": "break-word"})]),

        html.Div(className='row', style={'display': 'flex'}, children=[html.Div(
            dcc.Graph(id='live-graph', animate=False), className='col s12 m6 l6'), html.Div(
            dcc.Graph(id='world-map', animate=False), className='col s12 m6 l6')]),

        # html.Div(className='row', children=[html.Div(
        #     id="recent-tweets-table", className='col s12 m6 l6')]),

        html.Div(className='row', style={'display': 'flex'}, children=[
                 html.Div(id="recent-tweets-table", className='col s12 m6 l6'),
                 html.Div(dcc.Graph(id='sentiment-pie', animate=False), className='col s12 m6 l6')]),

        # html.Div(className='row', children=[html.Div(dcc.Graph(id='live-graph', animate=False), className='col s12 m6 l6'),
        #                                     html.Div(dcc.Graph(id='historical-graph', animate=False), className='col s12 m6 l6')]),

        # html.Div(className='row', children=[html.Div(
        #     id="recent-tweets-table", className='col s12 m6 l6')]),

        # html.Div(className='row', children=[html.Div(id="recent-tweets-table", className='col s12 m6 l6'),
        #                                     html.Div(dcc.Graph(id='sentiment-pie', animate=False), className='col s12 m6 l6'), ]),

     dcc.Interval(
        id='live-graph-update',
        interval=1*1000
    ),

        dcc.Interval(
        id='recent-tweets-table-update',
        interval=2*1000
    ),

        dcc.Interval(
            id='world-map-update',
            interval=1*1000
    ),
        #     dcc.Interval(
        #         id='historical-update',
        #         interval=60*1000
        # ),

        #     dcc.Interval(
        #         id='related-update',
        #         interval=30*1000
        # ),



        dcc.Interval(
        id='sentiment-pie-update',
        interval=60*1000
    ),

    ], style={'backgroundColor': color_palette['white'], 'margin-top': '-30px', 'height': '2000px', },
)


@ app.callback(Output('live-graph', 'figure'),
               [Input(component_id='sentiment_term', component_property='value'),
                Input('live-graph-update', 'n_intervals')])
def update_graph_scatter(sentiment_term, datac):
    try:
        conn = sqlite3.connect('twitter_live_bak.db')
        df = pd.read_sql(
            "SELECT * FROM sentiment WHERE tweet LIKE ? ORDER BY unix DESC LIMIT 1000", conn, params=('%' + sentiment_term + '%',))
        df.sort_values('unix', inplace=True)
        df['sentiment_smoothed'] = df['sentiment'].rolling(
            int(len(df)/5)).mean()
        df['date'] = pd.to_datetime(df['unix'], unit='ms')
        df.set_index('date', inplace=True)
        df = df.resample('300ms').mean()
        df.dropna(inplace=True)

        X = df.index[-100:]
        Y = df.sentiment_smoothed.values[-100:]

        data = plotly.graph_objs.Scatter(
            x=X,
            y=Y,
            name='Scatter',
            mode='lines+markers'
        )

        return {'data': [data], 'layout': go.Layout(xaxis=dict(range=[min(X), max(X)]),
                                                    yaxis=dict(
                                                        range=[min(Y), max(Y)]),
                                                    title='Term: {}'.format(sentiment_term))}

    except Exception as e:
        with open('errors.txt', 'a') as f:
            f.write(str(e))
            f.write('\n')


POS_NEG_NEUT = 0.1


def quick_color(s):
    # except return bg as app_colors['background']
    if s >= POS_NEG_NEUT:
        # positive
        return color_palette['aubergine_light']
    elif s <= -POS_NEG_NEUT:
        # negative:
        return color_palette['orange']

    else:
        return color_palette['grey_cool']


def generate_table(df, max_rows=10):
    return html.Table(className="responsive-table",
                      children=[
                          html.Thead(
                              html.Tr(
                                  children=[
                                      html.Th(col.title()) for col in df.columns.values],
                                  style={'color': color_palette['grey_text']}
                              )
                          ),
                          html.Tbody(
                              [

                                  html.Tr(
                                      children=[
                                          html.Td(data) for data in d
                                      ], style={'color': color_palette['white'],
                                                'background-color':quick_color(d[2])}
                                  )
                                  for d in df.values.tolist()])
                      ]
                      )


@ app.callback(Output('recent-tweets-table', 'children'),
               [Input(component_id='sentiment_term', component_property='value'),
                Input('recent-tweets-table-update', 'n_intervals')])
def update_recent_tweets(sentiment_term, datac):
    try:
        conn = sqlite3.connect('twitter_live_bak.db')
        if sentiment_term:
            df = pd.read_sql("SELECT sentiment.* FROM sentiment_fts fts LEFT JOIN sentiment ON fts.rowid = sentiment.id WHERE fts.sentiment_fts MATCH ? ORDER BY fts.rowid DESC LIMIT 10",
                             conn, params=(sentiment_term+'*',))
        else:
            df = pd.read_sql(
                "SELECT * FROM sentiment ORDER BY id DESC, unix DESC LIMIT 10", conn)

        df['date'] = pd.to_datetime(df['unix'], unit='ms')

        df = df.drop(['unix', 'id'], axis=1)
        df = df[['date', 'tweet', 'sentiment']]

        return generate_table(df, max_rows=10)
    except Exception as e:
        with open('errors.txt', 'a') as f:
            f.write(str(e))
            f.write('\n')


@ app.callback(Output('world-map', 'figure'),
               [Input(component_id='sentiment_term', component_property='value'),
                Input('world-map-update', 'n_intervals')])
def update_map(sentiment_term, datac):

    df = pd.read_csv(
        '2014_world_gdp_with_codes.csv')

    try:
        fig = go.Figure(data=go.Choropleth(
            locations=df['CODE'],
            z=df['GDP (BILLIONS)'],
            text=df['COUNTRY'],
            colorscale='Rainbow',  # Rainbow, Blues, Reds
            autocolorscale=False,
            reversescale=True,
            marker_line_color='darkgray',
            marker_line_width=0.5,
            colorbar_tickprefix='%',
            colorbar_title='Positive<br>Sentiment<br>Intensity',
        ))

        fig.update_layout(
            title_text=sentiment_term,
            geo=dict(
                showframe=False,
                showcoastlines=False,
                projection_type='equirectangular'
            ),
            annotations=[dict(
                x=0.55,
                y=0.1,
                xref='paper',
                yref='paper',
                text='Source: <a href="https://www.cia.gov/library/publications/the-world-factbook/fields/2195.html">\
                    CIA World Factbook</a>',
                showarrow=False
            )],
        )

    except:
        pass

    return (fig)


@app.callback(Output('sentiment-pie', 'figure'),
              [Input(component_id='sentiment_term', component_property='value'),
               Input('sentiment-pie-update', 'n_intervals')])
def update_pie_chart(sentiment_term, datac):

    # get data from cache
    for _ in range(100):
        sentiment_pie_dict = cache.get('sentiment_shares', sentiment_term)
        if sentiment_pie_dict:
            break
        time.sleep(0.1)

    if not sentiment_pie_dict:
        return None

    labels = ['Positive', 'Negative']

    try:
        pos = sentiment_pie_dict[1]
    except:
        pos = 0

    try:
        neg = sentiment_pie_dict[-1]
    except:
        neg = 0

    values = [pos, neg]
    colors = [color_palette['aubergine_light'], color_palette['orange']]

    trace = go.Pie(labels=labels, values=values,
                   hoverinfo='label+percent', textinfo='value',
                   textfont=dict(size=20, color=color_palette['black']),
                   marker=dict(colors=colors,
                               line=dict(color=color_palette['black'], width=2)))

    return {"data": [trace], 'layout': go.Layout(
        title='Positive vs Negative sentiment for "{}" (longer-term)'.format(
            sentiment_term),
        font={'color': color_palette['black']},
        plot_bgcolor=color_palette['black'],
        paper_bgcolor=color_palette['black'],
        showlegend=True)}


if __name__ == '__main__':
    app.run_server(debug=True)
