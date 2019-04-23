import React, { Component } from 'react';
import logo from './logo.svg';
import './styles/App.css';
import { Form, Input } from 'reactstrap';

const config = require('./config/server');
const axios = require('axios');

class App extends Component {
  constructor () {
    super()
    this.state = {query: ""}
  }
  componentDidMount () {
    axios.get(config.address+'query/google', { crossdomain: true })
    .then(function (response) {
      console.log("hi");
      console.log(response);
    })
    .catch(function (error) {
      console.log(config.address);
      console.log(error);
    })
  }
  handleSearch = (ev) => {
    console.log(this.state.query)
    // redirect to search + results page
    // with this query
  }
  handleChange = (ev) => {
    this.setState({
      query: ev.target.value
    });
  }
  render() {
    return (
      <div className="App">
        <div><h1>Hello,</h1>
        <Form onSubmit={this.handleSearch}>
          <Input type="search" className="searchbox--main" placeholder="Enter a query here" onChange={this.handleChange} />
        </Form></div>
      </div>
    );
  }
}

export default App;
