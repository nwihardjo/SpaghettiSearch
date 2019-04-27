import React, { Component } from 'react';
import ResultCard from './ResultCard';
import '../styles/Results.css';

const config = require('../config/server');
const axios = require('axios');

class Results extends Component {
  constructor (props) {
    super(props);
    this.state = {query:"", results: []}
  }
  componentDidMount (props) {
    this.setState({query: this.props.query});
    this.getResults(this.props.query);
  }
  // getResults = (query) => {
  //   this._getResults(query);
  // }
  getResults = (query) => {
    axios.get(config.address+'query/google')
    .then((response) => {
      console.log(query, response.data);
      this.setState({query: query, results: response.data});
    })
    .catch((error) => {
      console.log(error);
    })
  }
  // displayResults = () => {
  //   let layout = []
  //   for(let i=0; i<len(this.state.results);i++){
  //     layout.push(<ResultCard content={this.state.results[i]}>);
  //   }
  // }
  render() {
    console.log("rendere");
    return (
      <div className="results">
      {
        this.state.results.map((data, i) => {
          return(
            <ResultCard data={data}/>
          );
        })
      }
      </div>
    );
  }
}

export default Results;
