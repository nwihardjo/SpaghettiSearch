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

  getResults = (query) => {
    this.setState({results: []});
    console.log("Results received query: ", query)
    axios({
      method: 'post',
      url: config.address+'query',
      data: {
        Query: query
      },
      headers: {
        'Content-Type': 'application/json'
      }
    })
    .then((response) => {
      console.log(response.data);
      var filtered = response.data.filter(function (el) {
        return el.Url != "";
      })
      this.setState({query: query, results: filtered});
    })
    .catch((error) => {
      console.log(error);
    });
  }
  render() {
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
