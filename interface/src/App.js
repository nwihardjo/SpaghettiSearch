import React from "react";
import Main from "./containers/Main";
import ResultsContainer from "./containers/ResultsContainer";
import WordList from "./containers/WordList";
import { BrowserRouter as Router, Route, Link, Switch, withRouter } from "react-router-dom";
import history from './utils/history';

function App() {
  return (
    <Router history={history}>
    <Switch>
        <Route path="/" exact component={withRouter(Main)}/>
        <Route path="/query" exact component={withRouter(ResultsContainer)} />
        <Route path="/word-list" component={withRouter(WordList)} />
        </Switch>
    </Router>
  );
}

export default App;
