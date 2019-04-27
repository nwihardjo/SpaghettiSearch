import React, { Component } from 'react';
import Results from '../components/Results';
import {Nav, Navbar, NavbarBrand, NavLink, NavItem, Form, Input} from 'reactstrap';
import '../styles/ResultsContainer.css';

class ResultsContainer extends Component {
  constructor () {
    super()
    this.resultsElement = React.createRef();
    this.state = {query: "", results: []}
  }
  componentDidMount (props) {
    this.setState({query: this.props.query});
  }
  handleChange = (ev) => {
    this.setState({
      query: ev.target.value
    });
  }
  getResults = (ev) => {
    ev.preventDefault();
    this.resultsElement.current.getResults(this.state.query);
    // axios.get(config.address+'query/google', { crossdomain: true })
    // .then(function (response) {
    //   console.log(response.data);
    //   this.setState({results: response.data});
    // })
    // .catch(function (error) {
    //   console.log(config.address);
    //   console.log(error);
    // })
  }
  render() {
    return (
      <div>
      <Navbar color="faded" light className="header">
      <NavbarBrand><b>Query</b></NavbarBrand>
      <Nav className="mr-auto" navbar>
      <Form onSubmit={this.getResults}>
        <Input id='searchbar' type="search" className="searchbox--results" placeholder="What would you like to search?" defaultValue={this.state.query} onChange={this.handleChange}/>
      </Form>
      </Nav>
      </Navbar>
      <Results ref={this.resultsElement}/>
      </div>
    );
  }
}

export default ResultsContainer;
