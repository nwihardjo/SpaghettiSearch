import React, { Component } from 'react';
import {Card, CardText, CardBody, CardTitle, CardSubtitle, CardLink} from 'reactstrap';
import '../styles/ResultCard.css';
const config = require('../config/server');
const axios = require('axios');

class ResultCard extends Component {
  constructor (props) {
    super(props)
    this.state = {Url: "", Page_title: "", Page_size: 0, Children:[], Parents: [], Words_mapping:{}}
  }
  componentDidMount (props) {
    console.log("hi",this.props.data)
    this.setState({Url: this.props.data['Url'],
                  Page_title: this.props.data['Page_title'],
                  Page_size: this.props.data['Page_size'],
                  Children: this.props.data['Children'],
                  Parents: this.props.data['Parents'],
                  Words_mapping: this.props.data['Words_mapping']});
  }
  render() {
    return (
      <Card className='custom'>
        <CardBody>
          <CardLink className='title' href={this.state.Url}> {this.state.Page_title} </CardLink>
          <CardSubtitle> {this.state.Url} </CardSubtitle>
        </CardBody>
        <CardBody>
          <CardText>
          Lorem ipsum dolor sit amet, consectetur adipiscing elit. Quisque dictum nulla vitae elit tempus varius. Duis vitae tellus dapibus, convallis tortor sit amet, hendrerit magna. 
          <small className="text-muted">            {this.state.Page_size}
</small>
          </CardText>
        </CardBody>
      </Card>
    );
  }
}

export default ResultCard;
