import React, { Component } from 'react';

const config = require('../config/server');
const axios = require('axios');

class Results extends Component {
  constructor () {
    super()
    this.state = {query: "", results:[]}
  }
  componentDidMount (props) {
    this.setState({query: this.props.query});
    this.getResults();
  }
  getResults = () => {
    axios.get(config.address+'query/google', { crossdomain: true })
    .then(function (response) {
      console.log(response.data);
    })
    .catch(function (error) {
      console.log(config.address);
      console.log(error);
    })
  }
  render() {
    return (
      <div>
      </div>
    );
  }
}

export default Results;
