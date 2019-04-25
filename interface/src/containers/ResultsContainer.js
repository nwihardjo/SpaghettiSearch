import React, { Component } from 'react';
import Results from '../components/Results';
import {Form, Input} from 'reactstrap';

class ResultsContainer extends Component {
  constructor () {
    super()
    this.state = {query: ""}
  }
  componentDidMount (props) {
    this.setState({query: this.props.query});
  }
  handleChange = (ev) => {

  }
  render() {
    return (
      <div>
      <Form>
        <Input type="search" className="searchbox--results" placeholder="What would you like to search?" defaultValue={this.state.query} onChange={this.handleChange}/>
      </Form>
      <Results query={this.state.query}/>
      </div>
    );
  }
}

export default ResultsContainer;
