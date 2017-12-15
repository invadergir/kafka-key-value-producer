package com.example

import org.scalatest.{Matchers, FunSpec}

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

abstract class UnitSpec 
  extends FunSpec 
  with Matchers 
  with BeforeAndAfterAll
  with BeforeAndAfterEach


