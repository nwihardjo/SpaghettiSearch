import React, { Component } from 'react';

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
      <div>{this.state.word} {this.state.freq}</div>
    );
  }
}

export default Keywords;
