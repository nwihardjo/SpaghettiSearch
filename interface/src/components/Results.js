import React, { Component } from 'react';
import ResultCard from './ResultCard';
import '../styles/Results.css';

const config = require('../config/server');
const axios = require('axios');

class Results extends Component {
  constructor (props) {
    super(props);
    this.state = {query:"", results: [], terms: []}
  }
  componentDidMount (props) {
    this.setState({query: this.props.query});
    this.getResults(this.props.query);
  }

  getResults = (query) => {
    this.setState({results: [], query: query});
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
      var filtered = response.data.filter(function (el) {
        return el.Url != "";
      });

			var regex = /".*?"/g;
			var oldQuery = this.state.query;
			var phrases = this.state.query.match(regex);
			let pLen = !phrases ? 0 : phrases.length;
			var terms = [];
			for(let i=0; i < pLen; i++) {
				oldQuery = oldQuery.replace(phrases[i], "");
				phrases[i] = phrases[i].slice(1, -1);
				if(phrases[i].length !== 0) {
					terms.push(phrases[i]);
				}
			}
			let tokens = oldQuery.split(" ");
			for(let i=0; i < tokens.length; i++) {
				let t = tokens[i].trim();
				if(t.length !== 0) {
					terms.push(t);
				}
			}

			console.log(terms);
      this.setState({query: query, results: filtered, terms: terms });
    })
    .catch((error) => {
    });
  }
  render() {
    return (
      <div className="results">
      {
        this.state.results.map((data, i) => {
          return(
            <ResultCard data={data} terms={this.state.terms}/>
          );
        }, this)
      }
      </div>
    );
  }
}

export default Results;
