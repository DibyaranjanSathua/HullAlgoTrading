"""
File:           app.py
Author:         Dibyaranjan Sathua
Created on:     21/04/22, 1:29 pm
"""
import base64
from pathlib import Path

from dash import Dash, html, dcc
from dash.dependencies import Input, Output, State
from dash.long_callback import DiskcacheLongCallbackManager
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc
import diskcache

from src.backtesting import HullMABackTesting, RuleEngine2, RuleEngine3
from src.backtesting.strategy_analysis import StrategyAnalysis, ConsecutiveWinLoss


APP_DATA_DIR = Path(__file__).absolute().parents[2] / "data"
cache = diskcache.Cache("./cache")
long_callback_manager = DiskcacheLongCallbackManager(cache)

app = Dash(
    external_stylesheets=[dbc.themes.DARKLY],
    meta_tags=[
        {"name": "viewport", "content": "width=device-width, initial-scale=1"},
    ],
    long_callback_manager=long_callback_manager
)


ENGINE_MAPPER = {
    "rule_engine1": HullMABackTesting,
    "rule_engine2": RuleEngine2,
    "rule_engine3": RuleEngine3,
}


class AppLayout:
    """ Class for App layout """

    def __init__(self):
        pass

    def setup(self):
        """ Setup the app layout """
        # Don't assign to the function output. Assign it to the function object so that whenever we
        # do some changes to layout, it will reflect without server restarting
        app.layout = self.get_root_layout
        return app.server

    def get_root_layout(self):
        """ Return main page layout """
        # Empty dataclass used to generate default table
        strategy_analysis = StrategyAnalysis()
        strategy_analysis.consecutive_win_loss = ConsecutiveWinLoss()
        layout = dbc.Container(
            fluid=True,
            children=[
                self.get_nav_bar_layout(),
                dbc.Row(
                    [
                        dbc.Col(children=self.get_input_layout(), md=2),
                        dbc.Col(
                            [
                                dbc.Row(html.H4("CE Buy Analysis")),
                                dbc.Row(
                                    children=AppLayout.get_strategy_analysis_table(strategy_analysis),
                                    id="ce_analysis_table"
                                )
                            ],
                            md=5,
                        ),
                        dbc.Col(
                            [
                                dbc.Row(html.H4("PE Sell Analysis")),
                                dbc.Row(
                                    children=AppLayout.get_strategy_analysis_table(strategy_analysis),
                                    id="pe_analysis_table"
                                )
                            ],
                            md=5,
                        ),
                    ]
                ),
                dbc.Row(
                    dbc.Label(children="Made with ❤️ in India by SathuaLabs"),
                    className="justify-content-center"
                ),
                dbc.Row(
                    html.Div(
                        [
                            html.Label(children="", id="input_excel_file_path", hidden=True),
                            html.Label(children="", id="output_excel_file_path", hidden=True),
                            html.Label(children="", id="input_config_file_path", hidden=True)
                        ],
                    ),
                )
            ]
        )
        return layout

    @staticmethod
    def get_nav_bar_layout():
        """ Nav bar layout """
        return dbc.Navbar(
            children=[
                html.A(
                    # Use row and col to control vertical alignment of logo / brand
                    dbc.Row(
                        [
                            dbc.Col(dbc.NavbarBrand("BackTesting Framework", className="ml-4")),
                            dbc.Col(
                                dbc.Alert(
                                    children="Error",
                                    id="error_alert",
                                    is_open=False,
                                    dismissable=True,
                                    color="danger"
                                )
                            ),
                        ],
                        align="center",
                    ),
                    href="#",
                ),
            ],
            color="primary",
            dark=True,
            sticky="top",
            className="mb-2"
        )

    @staticmethod
    def get_input_layout():
        """ Provide input for backtesting """
        return dbc.Card(
            [
                dcc.Upload(
                    dbc.Button("Upload input excel file", color="warning", class_name="m-2"),
                    id="upload_excel",
                    multiple=False
                ),
                dbc.Input(id="excel_input", disabled=True, class_name="m-2"),
                dcc.Upload(
                    dbc.Button("Upload config file", color="warning", class_name="m-2"),
                    id="upload_config",
                    multiple=False
                ),
                dbc.Input(id="config_input", disabled=True, class_name="m-2"),
                dbc.Select(
                    id="rule_engine",
                    options=[
                        {"label": "Rule Engine1", "value": "rule_engine1"},
                        {"label": "Rule Engine2", "value": "rule_engine2"},
                        {"label": "Rule Engine3", "value": "rule_engine3"},
                    ],
                    value="rule_engine1",
                ),
                dbc.Button(
                    "Run BackTesting",
                    color="primary",
                    id="run_back_testing_btn",
                    class_name="my-4"
                ),
                dbc.Button(
                    "Download Excel",
                    color="primary",
                    id="download_excel_btn",
                    class_name="my-2",
                    # disabled=True
                ),
                dcc.Download(id="download_excel")
            ]
        )

    @staticmethod
    @app.long_callback(
        [
            Output(component_id="ce_analysis_table", component_property="children"),
            Output(component_id="pe_analysis_table", component_property="children"),
            Output(component_id="error_alert", component_property="children"),
            Output(component_id="error_alert", component_property="is_open")
        ],
        Input(component_id="run_back_testing_btn", component_property="n_clicks"),
        [
            State(component_id="input_excel_file_path", component_property="children"),
            State(component_id="output_excel_file_path", component_property="children"),
            State(component_id="input_config_file_path", component_property="children"),
            State(component_id="rule_engine", component_property="value"),
        ],
        running=[
            (
                    Output("run_back_testing_btn", "children"),
                    [dbc.Spinner(size="sm"), " Running..."],
                    "Run BackTesting"
            ),
            (
                Output("run_back_testing_btn", "disabled"),
                True,
                False
            )
        ],
        prevent_initial_call=True
    )
    def run_back_testing(
            n_clicks,
            input_excel_file_path: str,
            output_excel_file_path: str,
            config_file_path: str,
            rule_engine: str
    ):
        """ Run back testing"""
        # Empty dataclass used to generate default table
        strategy_analysis = StrategyAnalysis()
        strategy_analysis.consecutive_win_loss = ConsecutiveWinLoss()
        if not input_excel_file_path or not config_file_path:
            err = "Either input excel file or config file is missing"
            return AppLayout.get_strategy_analysis_table(strategy_analysis), \
                   AppLayout.get_strategy_analysis_table(strategy_analysis), str(err), True
        engine = ENGINE_MAPPER.get(rule_engine)
        strategy = engine(
            config_file_path=config_file_path,
            input_excel_file_path=input_excel_file_path,
            output_excel_file_path=output_excel_file_path
        )
        try:
            strategy.execute()
            if engine is RuleEngine3:
                ce_strategy_analysis = strategy.strategy_analysis
                pe_strategy_analysis = strategy_analysis
            else:
                ce_strategy_analysis = strategy.ce_strategy_analysis
                pe_strategy_analysis = strategy.pe_strategy_analysis
            return AppLayout.get_strategy_analysis_table(ce_strategy_analysis), \
                   AppLayout.get_strategy_analysis_table(pe_strategy_analysis), "", False
        except Exception as err:
            return AppLayout.get_strategy_analysis_table(strategy_analysis), \
                   AppLayout.get_strategy_analysis_table(strategy_analysis), str(err), True

    @staticmethod
    @app.callback(
        [
            Output(component_id="excel_input", component_property="value"),
            Output(component_id="input_excel_file_path", component_property="children"),
            Output(component_id="output_excel_file_path", component_property="children"),
        ],
        [
            Input(component_id="upload_excel", component_property="filename"),
            Input(component_id="upload_excel", component_property="contents")
        ],
        prevent_initial_call=True
    )
    def upload_excel(filename, contents):
        """ Save file to data dir """
        filepath = AppLayout.save_file(filename, contents)
        name, ext = filename.split(".")
        output_file_name = f"{name}_output.{ext}"
        output_file_path = APP_DATA_DIR / output_file_name
        return filename, filepath.as_posix(), output_file_path.as_posix()

    @staticmethod
    @app.callback(
        [
            Output(component_id="config_input", component_property="value"),
            Output(component_id="input_config_file_path", component_property="children"),
        ],
        [
            Input(component_id="upload_config", component_property="filename"),
            Input(component_id="upload_config", component_property="contents")
        ],
        prevent_initial_call=True
    )
    def upload_config(filename, contents):
        """ Save config file to data dir """
        filepath = AppLayout.save_file(filename, contents)
        return filename, filepath.as_posix()

    @staticmethod
    @app.callback(
        Output(component_id="download_excel", component_property="data"),
        Input(component_id="download_excel_btn", component_property="n_clicks"),
        State(component_id="output_excel_file_path", component_property="children"),
        prevent_initial_call=True,
    )
    def download_excel(n_clicks, output_excel_file_path):
        if n_clicks:
            if output_excel_file_path:
                return dcc.send_file(output_excel_file_path)
        raise PreventUpdate()

    @staticmethod
    def get_strategy_analysis_table(strategy_analysis: StrategyAnalysis):
        """ Strategy analysis """
        table_header = html.Thead(html.Tr(
                [
                    html.Th("Parameters"),
                    html.Th("Value")
                ]
        ))
        rows = list()
        rows.append(
            html.Tr([html.Td("Initial Capital"), html.Td(strategy_analysis.initial_capital)])
        )
        rows.append(html.Tr([html.Td("Lot Size"), html.Td(strategy_analysis.lot_size)]))
        rows.append(html.Tr([html.Td("Total Trades"), html.Td(strategy_analysis.total_trades)]))
        rows.append(html.Tr([html.Td("Profit/Loss"), html.Td(strategy_analysis.profit_loss)]))
        rows.append(html.Tr([html.Td("Ending Capital"), html.Td(strategy_analysis.ending_capital)]))
        rows.append(html.Tr([html.Td("Returns"), html.Td(strategy_analysis.capital_returns)]))
        rows.append(html.Tr([html.Td("Win Trades"), html.Td(strategy_analysis.win_trades)]))
        rows.append(html.Tr([html.Td("Win %"), html.Td(strategy_analysis.win_percent)]))
        rows.append(html.Tr([html.Td("Loss Trades"), html.Td(strategy_analysis.loss_trades)]))
        rows.append(html.Tr([html.Td("Loss %"), html.Td(strategy_analysis.loss_percent)]))
        rows.append(html.Tr([html.Td("Win Ratio"), html.Td(strategy_analysis.win_ratio)]))
        rows.append(html.Tr([html.Td("Avg Win"), html.Td(strategy_analysis.avg_win)]))
        rows.append(html.Tr([html.Td("Avg Loss"), html.Td(strategy_analysis.avg_loss)]))
        rows.append(
            html.Tr([html.Td("Avg Win / Avg Loss"), html.Td(strategy_analysis.avg_win_loss)])
        )
        rows.append(
            html.Tr([html.Td("Profit Potential"), html.Td(strategy_analysis.profit_potential)])
        )
        rows.append(html.Tr(
            [
                html.Td("Consecutive Wins"),
                html.Td(strategy_analysis.consecutive_win_loss.consecutive_win)
            ]
        ))
        rows.append(html.Tr(
            [
                html.Td("Consecutive Losses"),
                html.Td(strategy_analysis.consecutive_win_loss.consecutive_loss)
            ]
        ))
        rows.append(html.Tr(
            [
                html.Td("Maximun Drawdown"),
                html.Td(strategy_analysis.drawdown)
            ]
        ))
        table_body = html.Tbody(rows)
        return dbc.Table(
            [
                table_header,
                table_body
            ],
            bordered=True,
        )

    @staticmethod
    def save_file(filename, contents) -> Path:
        """ Save the uploaded file to App Data dir """
        data = contents.encode("utf8").split(b";base64,")[1]
        filepath = APP_DATA_DIR / filename
        with open(filepath, "wb") as fp:
            fp.write(base64.decodebytes(data))
        return filepath


if __name__ == "__main__":
    AppLayout().setup()
    app.run_server(debug=True)
