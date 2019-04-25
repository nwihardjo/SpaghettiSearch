import React, { Component } from 'react';
import ResultsContainer from './ResultsContainer';
import '../styles/Main.css';
import { Form, Input } from 'reactstrap';

class Main extends Component {
  constructor () {
    super()
    this.state = {query: "", showResults: false}
  }
  componentDidMount () {
  }
  handleSearch = (ev) => {
    // redirect to search + results page
    // with this query
    this.setState({showResults:true});
  }
  handleChange = (ev) => {
    this.setState({
      query: ev.target.value
    });
  }
  render() {
    if(this.state.showResults) {
      return (
        <ResultsContainer query={this.state.query}/>
      );
    } else {
      return (
        <div className="App">
          <div><h1>Hello,</h1>
          <Form onSubmit={this.handleSearch}>
            <Input type="search" className="searchbox--main" placeholder="What would you like to search?"  bsSize="lg" onChange={this.handleChange} />
          </Form></div>
        </div>
      );
    }
  }
}

export default Main;
