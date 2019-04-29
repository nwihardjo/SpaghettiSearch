import React from "react";
import Main from "./containers/Main";
import ResultsContainer from "./containers/ResultsContainer";
import WordList from "./containers/WordList";
import { BrowserRouter as Router, Route, Link } from "react-router-dom";

function App() {
  return (
    <Router>
        <Route path="/" exact component={Main} />
        <Route path="/query" exact component={ResultsContainer} />
        <Route path="/word-list" exact component={WordList} />
    </Router>
  );
}

export default App;
