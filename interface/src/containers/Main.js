import React, { Component } from 'react';
import ResultsContainer from './ResultsContainer';
import '../styles/Main.css';
import { Form, Input, Button } from 'reactstrap';

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
    ev.preventDefault();
    this.setState({
      query: ev.target.value
    });
    console.log(this.state.query)
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
          </Form><br/>
          <small className="text-muted">
            OR TRY <a href='/word-list'><Button outline color="primary">Keyword Search</Button></a>
          </small></div>
        </div>
      );
    }
  }
}

export default Main;
