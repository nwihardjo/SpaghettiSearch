import React, { Component } from 'react';
import {Card, CardText} from 'reactstrap';

import '../styles/Keywords.css';

class Keywords extends Component {
  constructor (props) {
    super(props)
    this.state = {word: "", freq: 0}
  }
  componentDidMount (props) {
    this.setState({word: this.props.word, freq: this.props.freq});
  }
  render() {
    return (
      <div className='word'>
          {this.state.word} <span>&#8729;</span> {this.state.freq}
      </div>
    );
  }
}

export default Keywords;
